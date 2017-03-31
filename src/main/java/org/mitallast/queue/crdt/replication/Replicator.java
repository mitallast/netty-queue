package org.mitallast.queue.crdt.replication;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import gnu.trove.impl.sync.TSynchronizedObjectLongMap;
import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.common.Match;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.crdt.log.LogEntry;
import org.mitallast.queue.crdt.log.ReplicatedLog;
import org.mitallast.queue.crdt.protocol.*;
import org.mitallast.queue.raft.Raft;
import org.mitallast.queue.raft.RaftMetadata;
import org.mitallast.queue.raft.discovery.ClusterDiscovery;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportController;
import org.mitallast.queue.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class Replicator extends AbstractLifecycleComponent {
    private final static Logger logger = LogManager.getLogger();

    private final Raft raft;
    private final ClusterDiscovery clusterDiscovery;
    private final TransportService transportService;
    private final ReplicatedLog log;
    private final ReentrantLock lock = new ReentrantLock();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final TObjectLongMap<DiscoveryNode> vclock = new TSynchronizedObjectLongMap<>(new TObjectLongHashMap<>());
    private final TObjectLongMap<DiscoveryNode> replicationTimeout = new TSynchronizedObjectLongMap<>(new TObjectLongHashMap<>());

    private final long timeout;
    private final Match.Handler<Streamable> handler;

    @Inject
    public Replicator(
        Config config,
        Raft raft,
        ClusterDiscovery clusterDiscovery,
        TransportService transportService,
        TransportController transportController,
        ReplicatedLog log
    ) throws IOException {
        this.raft = raft;
        this.clusterDiscovery = clusterDiscovery;
        this.transportService = transportService;
        this.log = log;

        timeout = config.getDuration("crdt.timeout", TimeUnit.MILLISECONDS);
        handler = Match.<Streamable>handle()
            .when(Append.class, this::append)
            .when(AppendSuccessful.class, this::successful)
            .when(AppendRejected.class, this::rejected)
            .build();

        transportController.registerMessageHandler(AppendSuccessful.class, this::handle);
        transportController.registerMessageHandler(AppendRejected.class, this::handle);
    }

    public void handle(Streamable event) {
        lock.lock();
        try {
            handler.accept(event);
        } catch (IOException e) {
            logger.error(e);
        } finally {
            lock.unlock();
        }
    }

    private void initialize() throws IOException {
        RaftMetadata meta = raft.currentMeta();
        ImmutableSet<DiscoveryNode> members = meta.membersWithout(clusterDiscovery.self());
        for (DiscoveryNode member : members) {
            replicationTimeout.put(member, System.currentTimeMillis() + timeout);
            transportService.connectToNode(member);
            transportService.channel(member).send(new AppendEntries(clusterDiscovery.self(), 0, ImmutableList.of()));
        }
        scheduler.scheduleWithFixedDelay(() -> {
            lock.lock();
            try {
                maybeSendEntries();
            } catch (IOException e) {
                logger.error("error send entries", e);
            } finally {
                lock.unlock();
            }
        }, timeout, timeout, TimeUnit.MILLISECONDS);
    }

    private void append(Append append) throws IOException {
        log.append(append.id(), append.event());
        maybeSendEntries();
    }

    private void successful(AppendSuccessful message) throws IOException {
        logger.debug("append successful {} vclock={}", message.member(), message.vclock());
        if (vclock.get(message.member()) < message.vclock()) {
            vclock.put(message.member(), message.vclock());
        }
        replicationTimeout.put(message.member(), 0);
        maybeSendEntries(message.member());
    }

    private void rejected(AppendRejected message) throws IOException {
        logger.debug("append rejected {} vclock={}", message.member(), message.vclock());
        if (vclock.get(message.member()) < message.vclock()) {
            vclock.put(message.member(), message.vclock());
        }
        replicationTimeout.put(message.member(), 0);
        maybeSendEntries(message.member());
    }

    private void maybeSendEntries() throws IOException {
        ImmutableSet<DiscoveryNode> members = raft.currentMeta().membersWithout(clusterDiscovery.self());
        for (DiscoveryNode member : members) {
            maybeSendEntries(member);
        }
    }

    private void maybeSendEntries(DiscoveryNode member) throws IOException {
        List<LogEntry> entries = log.entries();
        if (!entries.isEmpty()) {
            LogEntry last = entries.get(entries.size() - 1);
            long nodeVclock = vclock.get(member);
            if (last.vclock() > nodeVclock) {
                long nodeTimeout = replicationTimeout.get(member);
                if (nodeTimeout < System.currentTimeMillis()) {
                    ImmutableList.Builder<LogEntry> builder = ImmutableList.builder();
                    for (int i = entries.size() - 1; i >= 0; i--) {
                        LogEntry logEntry = entries.get(i);
                        if (logEntry.vclock() > nodeVclock) {
                            builder.add(logEntry);
                        } else {
                            break;
                        }
                    }
                    ImmutableList<LogEntry> append = builder.build().reverse();
                    logger.debug("send append {} vclock={}", member, nodeVclock);
                    replicationTimeout.put(member, System.currentTimeMillis() + timeout);
                    transportService.connectToNode(member);
                    transportService.channel(member)
                        .send(new AppendEntries(clusterDiscovery.self(), nodeVclock, append));
                }
            }
        }
    }

    @Override
    protected void doStart() throws IOException {
        lock.lock();
        try {
            initialize();
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void doStop() throws IOException {

    }

    @Override
    protected void doClose() throws IOException {

    }
}
