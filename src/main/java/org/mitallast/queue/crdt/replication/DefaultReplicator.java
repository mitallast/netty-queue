package org.mitallast.queue.crdt.replication;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.typesafe.config.Config;
import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import javaslang.collection.Set;
import javaslang.collection.Vector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.crdt.bucket.Bucket;
import org.mitallast.queue.crdt.log.LogEntry;
import org.mitallast.queue.crdt.log.ReplicatedLog;
import org.mitallast.queue.crdt.protocol.AppendEntries;
import org.mitallast.queue.crdt.protocol.AppendRejected;
import org.mitallast.queue.crdt.protocol.AppendSuccessful;
import org.mitallast.queue.raft.Raft;
import org.mitallast.queue.raft.RaftMetadata;
import org.mitallast.queue.raft.discovery.ClusterDiscovery;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportService;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class DefaultReplicator extends AbstractLifecycleComponent implements Replicator {
    private final static Logger logger = LogManager.getLogger();

    private final Raft raft;
    private final ClusterDiscovery clusterDiscovery;
    private final TransportService transportService;
    private final Bucket bucket;

    private final ReentrantLock lock = new ReentrantLock();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final TObjectLongMap<DiscoveryNode> vclock = new TObjectLongHashMap<>();
    private final TObjectLongMap<DiscoveryNode> replicationTimeout = new TObjectLongHashMap<>();

    private final long timeout;

    @Inject
    public DefaultReplicator(
        Config config,
        Raft raft,
        ClusterDiscovery clusterDiscovery,
        TransportService transportService,
        @Assisted Bucket bucket
    ) {
        this.raft = raft;
        this.clusterDiscovery = clusterDiscovery;
        this.transportService = transportService;
        this.bucket = bucket;

        timeout = config.getDuration("crdt.timeout", TimeUnit.MILLISECONDS);
    }

    private void initialize() {
        RaftMetadata meta = raft.currentMeta();
        Set<DiscoveryNode> members = meta.membersWithout(clusterDiscovery.self());
        for (DiscoveryNode member : members) {
            replicationTimeout.put(member, System.currentTimeMillis() + timeout);
            AppendEntries appendEntries = new AppendEntries(bucket.index(), clusterDiscovery.self(), 0, Vector.empty());
            transportService.send(member, appendEntries);
        }
        scheduler.scheduleWithFixedDelay(() -> {
            lock.lock();
            try {
                maybeSendEntries();
            } finally {
                lock.unlock();
            }
        }, timeout, timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public void append(long id, Streamable event) {
        lock.lock();
        try {
            bucket.log().append(id, event);
            maybeSendEntries();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void successful(AppendSuccessful message) {
        lock.lock();
        try {
            logger.debug("append successful {} vclock={}", message.member(), message.vclock());
            if (vclock.get(message.member()) < message.vclock()) {
                vclock.put(message.member(), message.vclock());
            }
            replicationTimeout.put(message.member(), 0);
            maybeSendEntries(message.member());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rejected(AppendRejected message) {
        lock.lock();
        try {
            logger.warn("append rejected {} vclock={}", message.member(), message.vclock());
            if (vclock.get(message.member()) < message.vclock()) {
                vclock.put(message.member(), message.vclock());
            }
            replicationTimeout.put(message.member(), 0);
            maybeSendEntries(message.member());
        } finally {
            lock.unlock();

        }
    }

    private void maybeSendEntries() {
        Set<DiscoveryNode> members = raft.currentMeta().membersWithout(clusterDiscovery.self());
        for (DiscoveryNode member : members) {
            maybeSendEntries(member);
        }
    }

    private void maybeSendEntries(DiscoveryNode member) {
        long nodeTimeout = replicationTimeout.get(member);
        if (nodeTimeout < System.currentTimeMillis()) {
            sendEntries(member);
        }
    }

    private void sendEntries(DiscoveryNode member) {
        long nodeVclock = vclock.get(member);
        ReplicatedLog log = bucket.log();
        Vector<LogEntry> append = log.entriesFrom(nodeVclock);
        if (!append.isEmpty()) {
            logger.debug("send append {} vclock={}", member, nodeVclock);
            replicationTimeout.put(member, System.currentTimeMillis() + timeout);
            transportService.send(member, new AppendEntries(bucket.index(), clusterDiscovery.self(), nodeVclock, append));
        }
    }

    @Override
    protected void doStart() {
        lock.lock();
        try {
            initialize();
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() {}
}