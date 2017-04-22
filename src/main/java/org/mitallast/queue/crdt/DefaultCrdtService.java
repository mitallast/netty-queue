package org.mitallast.queue.crdt;

import javaslang.collection.HashMap;
import javaslang.collection.Map;
import javaslang.concurrent.Future;
import javaslang.control.Option;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.common.events.EventBus;
import org.mitallast.queue.crdt.bucket.Bucket;
import org.mitallast.queue.crdt.bucket.BucketFactory;
import org.mitallast.queue.crdt.event.ClosedLogSynced;
import org.mitallast.queue.crdt.log.LogEntry;
import org.mitallast.queue.crdt.protocol.AppendEntries;
import org.mitallast.queue.crdt.protocol.AppendRejected;
import org.mitallast.queue.crdt.protocol.AppendSuccessful;
import org.mitallast.queue.crdt.routing.*;
import org.mitallast.queue.crdt.routing.allocation.AllocationStrategy;
import org.mitallast.queue.crdt.routing.event.RoutingTableChanged;
import org.mitallast.queue.crdt.routing.fsm.*;
import org.mitallast.queue.raft.Raft;
import org.mitallast.queue.raft.discovery.ClusterDiscovery;
import org.mitallast.queue.raft.event.MembersChanged;
import org.mitallast.queue.raft.protocol.ClientMessage;
import org.mitallast.queue.transport.TransportController;
import org.mitallast.queue.transport.TransportService;

import javax.inject.Inject;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

import static org.mitallast.queue.raft.RaftState.Leader;

public class DefaultCrdtService implements CrdtService {
    private final Logger logger = LogManager.getLogger();
    private final Raft raft;
    private final RoutingTableFSM routingTableFSM;
    private final AllocationStrategy allocationStrategy;
    private final ClusterDiscovery discovery;
    private final BucketFactory bucketFactory;
    private final TransportService transportService;

    private final ReentrantLock lock = new ReentrantLock();
    private volatile long lastApplied = 0;
    private volatile Map<Integer, Bucket> buckets = HashMap.empty();

    @Inject
    public DefaultCrdtService(
        Raft raft,
        RoutingTableFSM routingTableFSM,
        AllocationStrategy allocationStrategy,
        ClusterDiscovery discovery,
        TransportController transportController,
        BucketFactory bucketFactory,
        EventBus eventBus,
        TransportService transportService
    ) {
        this.raft = raft;
        this.routingTableFSM = routingTableFSM;
        this.discovery = discovery;
        this.bucketFactory = bucketFactory;
        this.allocationStrategy = allocationStrategy;
        this.transportService = transportService;

        Executor executor = Executors.newSingleThreadExecutor();
        eventBus.subscribe(MembersChanged.class, this::handle, executor);
        eventBus.subscribe(RoutingTableChanged.class, this::handle, executor);
        eventBus.subscribe(ClosedLogSynced.class, this::handle, executor);

        transportController.registerMessageHandler(AppendEntries.class, this::append);
        transportController.registerMessageHandler(AppendSuccessful.class, this::successful);
        transportController.registerMessageHandler(AppendRejected.class, this::rejected);
    }

    private void append(AppendEntries message) {
        Bucket bucket = bucket(message.bucket());
        if (bucket == null) {
            logger.warn("unexpected bucket {}, ignore", message.bucket());
        } else {
            bucket.lock().lock();
            try {
                RoutingBucket routingBucket = routingTable().bucket(message.bucket());
                RoutingReplica replica = routingBucket.replicas().getOrElse(message.replica(), null);
                if (replica == null) {
                    logger.warn("unexpected replica {}, ignore", message.replica());
                } else {
                    long localIndex = bucket.vclock().get(message.replica());
                    if (localIndex == message.prevVclock()) {
                        for (LogEntry logEntry : message.entries()) {
                            bucket.registry().crdt(logEntry.id()).update(logEntry.event());
                            localIndex = Math.max(localIndex, logEntry.vclock());
                        }
                        bucket.vclock().put(message.replica(), localIndex);
                        if (logger.isDebugEnabled()) {
                            logger.debug("[replica={}:{}] append success to={}:{} prev={} new={}",
                                bucket.index(), bucket.replica(),
                                message.bucket(), message.replica(), message.prevVclock(), localIndex);
                        }
                        transportService.send(
                            replica.member(),
                            new AppendSuccessful(message.bucket(), bucket.replica(), localIndex)
                        );
                    } else {
                        logger.warn("[replica={}:{}] append reject to={}:{} prev={} index={}",
                            bucket.index(), bucket.replica(),
                            message.bucket(), message.replica(), message.prevVclock(), localIndex);
                        transportService.send(
                            replica.member(),
                            new AppendRejected(message.bucket(), bucket.replica(), localIndex)
                        );
                    }
                }
            } finally {
                bucket.lock().unlock();
            }
        }
    }

    private void successful(AppendSuccessful message) {
        Bucket bucket = bucket(message.bucket());
        if (bucket != null) {
            bucket.replicator().successful(message);
        }
    }

    private void rejected(AppendRejected message) {
        Bucket bucket = bucket(message.bucket());
        if (bucket != null) {
            bucket.replicator().rejected(message);
        }
    }

    private void handle(ClosedLogSynced message) {
        lock.lock();
        try {
            RoutingTable routingTable = routingTableFSM.get();
            Option<RoutingReplica> replica = routingTable.bucket(message.bucket())
                .replicas().get(message.replica());
            if (replica.isDefined() && replica.get().isClosed()) {
                logger.info("RemoveReplica bucket {} {}", message.bucket(), message.replica());
                RemoveReplica request = new RemoveReplica(message.bucket(), message.replica());
                raft.apply(new ClientMessage(request, 0));
            } else {
                logger.warn("open closed replicator bucket {}", message.bucket());
                Bucket bucket = bucket(message.bucket());
                if (bucket != null) {
                    bucket.replicator().open();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public RoutingTable routingTable() {
        return routingTableFSM.get();
    }

    public boolean contains(int index) {
        return buckets.containsKey(index);
    }

    @Override
    public Bucket bucket(int index) {
        return buckets.getOrElse(index, null);
    }

    @Override
    public Bucket bucket(long resourceId) {
        int index = routingTable().bucket(resourceId).index();
        return bucket(index);
    }

    @Override
    public Future<Boolean> addResource(long id, ResourceType resourceType) {
        return raft.command(new AddResource(id, resourceType))
            .filter(m -> m instanceof AddResourceResponse)
            .map(m -> ((AddResourceResponse) m).isCreated());
    }

    private void handle(MembersChanged event) {
        if (raft.currentState() == Leader) {
            logger.info("members changed");
            raft.apply(new ClientMessage(new UpdateMembers(event.members()), 0));
        }
    }

    private void handle(RoutingTableChanged changed) {
        lock.lock();
        try {
            if (changed.index() <= lastApplied) {
                return;
            }
            lastApplied = changed.index();
            logger.info("routing table changed: {}", changed.routingTable());
            processAsLeader(changed.routingTable());
            processBuckets(changed.routingTable());
        } finally {
            lock.unlock();
        }
    }

    private void processAsLeader(RoutingTable routingTable) {
        if (raft.currentState() == Leader) {
            allocationStrategy.update(routingTable)
                .forEach(cmd -> raft.apply(new ClientMessage(cmd, 0)));
        }
    }

    private void processBuckets(RoutingTable routingTable) {
        for (RoutingBucket routingBucket : routingTable.buckets()) {
            Option<RoutingReplica> replicaOpt = routingBucket.replicas().values()
                .find(r -> r.member().equals(discovery.self()));
            if (replicaOpt.isDefined()) {
                processReplica(routingBucket, replicaOpt.get());
            } else {
                deleteIfExists(routingBucket.index());
            }
        }
    }

    private void processReplica(RoutingBucket routingBucket, RoutingReplica replica) {
        Bucket bucket = bucket(routingBucket.index());
        if (bucket == null) {
            bucket = bucketFactory.create(routingBucket.index(), replica.id());
            buckets = buckets.put(routingBucket.index(), bucket);
        }
        bucket.lock().lock();
        try {
            if (replica.isClosed()) {
                bucket.replicator().closeAndSync();
            } else {
                bucket.replicator().open();
                for (Resource resource : routingBucket.resources().values()) {
                    if (bucket.registry().crdtOpt(resource.id()).isEmpty()) {
                        logger.info("allocate resource {}:{}", resource.id(), resource.type());
                        switch (resource.type()) {
                            case LWWRegister:
                                bucket.registry().createLWWRegister(resource.id());
                                break;
                            case GCounter:
                                bucket.registry().createGCounter(resource.id());
                                break;
                            default:
                                logger.warn("unexpected type: {}", resource.type());
                        }
                    }
                }
            }
        } finally {
            bucket.lock().unlock();
        }
    }

    private void deleteIfExists(int index) {
        Bucket bucket = bucket(index);
        if (bucket != null) {
            logger.info("delete bucket {}", index);
            bucket.lock().lock();
            try {
                buckets = buckets.remove(index);
                bucket.close();
                bucket.delete();
            } finally {
                bucket.lock().unlock();
            }
        }
    }
}
