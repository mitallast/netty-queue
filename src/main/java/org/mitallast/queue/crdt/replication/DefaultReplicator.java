package org.mitallast.queue.crdt.replication;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.typesafe.config.Config;
import gnu.trove.map.TLongLongMap;
import gnu.trove.map.hash.TLongLongHashMap;
import javaslang.collection.Seq;
import javaslang.collection.Vector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.events.EventBus;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.crdt.bucket.Bucket;
import org.mitallast.queue.crdt.event.ClosedLogSynced;
import org.mitallast.queue.crdt.log.LogEntry;
import org.mitallast.queue.crdt.log.ReplicatedLog;
import org.mitallast.queue.crdt.protocol.AppendEntries;
import org.mitallast.queue.crdt.protocol.AppendRejected;
import org.mitallast.queue.crdt.protocol.AppendSuccessful;
import org.mitallast.queue.crdt.routing.RoutingBucket;
import org.mitallast.queue.crdt.routing.RoutingReplica;
import org.mitallast.queue.crdt.routing.RoutingTable;
import org.mitallast.queue.crdt.routing.fsm.RoutingTableFSM;
import org.mitallast.queue.transport.TransportService;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class DefaultReplicator extends AbstractLifecycleComponent implements Replicator {
    private final static Logger logger = LogManager.getLogger();

    private final RoutingTableFSM fsm;
    private final EventBus eventBus;
    private final TransportService transportService;
    private final Bucket bucket;

    private final ReentrantLock lock = new ReentrantLock();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final TLongLongMap vclock = new TLongLongHashMap(32, 0.5f, 0, 0);
    private final TLongLongMap replicationTimeout = new TLongLongHashMap(32, 0.5f, 0, 0);

    private final long timeout;

    private volatile boolean open = true;

    @Inject
    public DefaultReplicator(
        Config config,
        RoutingTableFSM fsm,
        EventBus eventBus,
        TransportService transportService,
        @Assisted Bucket bucket
    ) {
        this.fsm = fsm;
        this.eventBus = eventBus;
        this.transportService = transportService;
        this.bucket = bucket;

        timeout = config.getDuration("crdt.timeout", TimeUnit.MILLISECONDS);
    }

    private void initialize() {
        RoutingTable routingTable = fsm.get();
        RoutingBucket routingBucket = routingTable.buckets().get(this.bucket.index());
        Seq<RoutingReplica> replicas = routingBucket.replicas().remove(bucket.replica()).values();

        for (RoutingReplica replica : replicas) {
            replicationTimeout.put(replica.id(), System.currentTimeMillis() + timeout);
            AppendEntries appendEntries = new AppendEntries(bucket.index(), bucket.replica(), 0, Vector.empty());
            transportService.send(replica.member(), appendEntries);
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
            if (!open) {
                throw new IllegalStateException("closed");
            }
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
            logger.debug("append successful replica={} vclock={}", message.replica(), message.vclock());
            if (vclock.get(message.replica()) < message.vclock()) {
                vclock.put(message.replica(), message.vclock());
            }
            replicationTimeout.put(message.replica(), 0);
            maybeSendEntries(message.replica());
            maybeSync();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rejected(AppendRejected message) {
        lock.lock();
        try {
            logger.warn("append rejected {} vclock={}", message.replica(), message.vclock());
            if (vclock.get(message.replica()) < message.vclock()) {
                vclock.put(message.replica(), message.vclock());
            }
            replicationTimeout.put(message.replica(), 0);
            maybeSendEntries(message.replica());
            maybeSync();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void open() {
        lock.lock();
        try {
            open = true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void closeAndSync() {
        lock.lock();
        try {
            open = false;
            maybeSync();
        } finally {
            lock.unlock();
        }
    }

    private void maybeSendEntries() {
        RoutingTable routingTable = fsm.get();
        RoutingBucket routingBucket = routingTable.buckets().get(this.bucket.index());
        routingBucket.replicas().remove(bucket.replica())
            .values().forEach(this::maybeSendEntries);
    }

    private void maybeSendEntries(long replica) {
        RoutingTable routingTable = fsm.get();
        RoutingBucket routingBucket = routingTable.buckets().get(this.bucket.index());
        routingBucket.replicas().get(replica).forEach(this::maybeSendEntries);
    }

    private void maybeSendEntries(RoutingReplica replica) {
        long timeout = replicationTimeout.get(replica.id());
        if (timeout < System.currentTimeMillis()) {
            sendEntries(replica);
        }
    }

    private void sendEntries(RoutingReplica replica) {
        long nodeVclock = vclock.get(replica.id());
        ReplicatedLog log = bucket.log();
        Vector<LogEntry> append = log.entriesFrom(nodeVclock);
        if (!append.isEmpty()) {
            logger.debug("send append replica={} vclock={}", replica.id(), nodeVclock);
            replicationTimeout.put(replica.id(), System.currentTimeMillis() + timeout);
            transportService.send(replica.member(), new AppendEntries(bucket.index(), bucket.replica(), nodeVclock, append));
        }
    }

    private void maybeSync() {
        if (!open) {
            long last = bucket.log().vclock();

            RoutingTable routingTable = fsm.get();
            RoutingBucket routingBucket = routingTable.buckets().get(bucket.index());
            Seq<RoutingReplica> replicas = routingBucket.replicas().remove(bucket.replica()).values();
            if (replicas.isEmpty()) { // no replica
                return;
            }
            for (RoutingReplica replica : replicas) {
                if (vclock.get(replica.id()) != last) {
                    return; // not synced
                }
            }
            eventBus.trigger(new ClosedLogSynced(bucket.index(), bucket.replica()));
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
