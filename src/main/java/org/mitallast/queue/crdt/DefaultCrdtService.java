package org.mitallast.queue.crdt;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.common.Immutable;
import org.mitallast.queue.common.events.EventBus;
import org.mitallast.queue.crdt.bucket.Bucket;
import org.mitallast.queue.crdt.bucket.BucketFactory;
import org.mitallast.queue.crdt.protocol.AppendRejected;
import org.mitallast.queue.crdt.protocol.AppendSuccessful;
import org.mitallast.queue.crdt.routing.Resource;
import org.mitallast.queue.crdt.routing.RoutingBucket;
import org.mitallast.queue.crdt.routing.RoutingTable;
import org.mitallast.queue.crdt.routing.event.RoutingTableChanged;
import org.mitallast.queue.crdt.routing.fsm.Allocate;
import org.mitallast.queue.crdt.routing.fsm.RoutingTableFSM;
import org.mitallast.queue.crdt.routing.fsm.UpdateMembers;
import org.mitallast.queue.raft.Raft;
import org.mitallast.queue.raft.discovery.ClusterDiscovery;
import org.mitallast.queue.raft.event.MembersChanged;
import org.mitallast.queue.raft.protocol.ClientMessage;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportController;

import javax.inject.Inject;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;

import static org.mitallast.queue.raft.RaftState.Leader;

public class DefaultCrdtService implements CrdtService {
    private final Logger logger = LogManager.getLogger();
    private final Raft raft;
    private final RoutingTableFSM routingTableFSM;
    private final ClusterDiscovery discovery;
    private final BucketFactory bucketFactory;

    private final ReentrantLock lock;
    private volatile long lastApplied = 0;
    private volatile ImmutableMap<Integer, Bucket> buckets = ImmutableMap.of();

    @Inject
    public DefaultCrdtService(
        Raft raft,
        RoutingTableFSM routingTableFSM,
        ClusterDiscovery discovery,
        TransportController transportController,
        BucketFactory bucketFactory,
        EventBus eventBus
    ) {
        this.raft = raft;
        this.routingTableFSM = routingTableFSM;
        this.discovery = discovery;
        this.bucketFactory = bucketFactory;

        this.lock = new ReentrantLock();

        Executor executor = Executors.newSingleThreadExecutor();
        eventBus.subscribe(MembersChanged.class, this::handle, executor);
        eventBus.subscribe(RoutingTableChanged.class, this::handle, executor);

        transportController.registerMessageHandler(AppendSuccessful.class, this::successful);
        transportController.registerMessageHandler(AppendRejected.class, this::rejected);
    }

    private void successful(AppendSuccessful message) {
        Bucket bucket = bucket(message.bucket());
        if (bucket != null) {
            try {
                bucket.replicator().successful(message);
            } catch (IOException e) {
                logger.error("error handle append successful", e);
            }
        }
    }

    private void rejected(AppendRejected message) {
        Bucket bucket = bucket(message.bucket());
        if (bucket != null) {
            try {
                bucket.replicator().rejected(message);
            } catch (IOException e) {
                logger.error("error handle append rejected", e);
            }
        }
    }

    @Override
    public RoutingTable routingTable() {
        return routingTableFSM.get();
    }

    @Override
    public boolean contains(int index) {
        return buckets.containsKey(index);
    }

    @Override
    public Bucket bucket(int index) {
        return buckets.get(index);
    }

    @Override
    public Bucket bucket(long resourceId) {
        int index = routingTable().bucket(resourceId).index();
        return bucket(index);
    }

    @Override
    public ImmutableCollection<Bucket> buckets() {
        return buckets.values();
    }

    private void handle(MembersChanged event) {
        if (raft.currentState() == Leader) {
            logger.info("members changed");
            raft.apply(new ClientMessage(discovery.self(), new UpdateMembers(event.members())));
        }
    }

    private void handle(RoutingTableChanged changed) {
        lock.lock();
        try {
            if (changed.index() <= lastApplied) {
                return;
            }
            lastApplied = changed.index();
            logger.info("routing table changed");
            processAsLeader(changed.routingTable());
            processBuckets(changed.routingTable());
        } finally {
            lock.unlock();
        }
    }

    private void processAsLeader(RoutingTable routingTable) {
        if (raft.currentState() == Leader) {
            ImmutableSet<DiscoveryNode> members = routingTable.members();
            for (RoutingBucket routingBucket : routingTable.buckets()) {
                if (routingBucket.members().size() < routingTable.replicas()) {
                    ImmutableSet<DiscoveryNode> bucketMembers = routingBucket.members();
                    ImmutableList<DiscoveryNode> available = Immutable.filterNot(members, bucketMembers::contains).asList();
                    if (!available.isEmpty()) {
                        DiscoveryNode node = available.get(ThreadLocalRandom.current().nextInt(available.size()));
                        logger.info("allocate bucket {} {} {}", routingBucket.index(), node);
                        Allocate allocate = new Allocate(routingBucket.index(), node);
                        raft.apply(new ClientMessage(discovery.self(), allocate));
                        return;
                    }
                }
            }
        }
    }

    private void processBuckets(RoutingTable routingTable) {
        for (RoutingBucket routingBucket : routingTable.buckets()) {
            if (routingBucket.members().contains(discovery.self())) {
                Bucket bucket = getOrCreate(routingBucket.index());
                for (Resource resource : routingBucket.resources().values()) {
                    if (!bucket.registry().crdtOpt(resource.id()).isPresent()) {
                        logger.info("allocate resource {}:{}", resource.id(), resource.type());
                        switch (resource.type()) {
                            case LWWRegister:
                                bucket.registry().createLWWRegister(resource.id());
                                break;
                            default:
                                logger.warn("unexpected type: {}", resource.type());
                        }
                    }
                }
            } else {
                try {
                    deleteIfExists(routingBucket.index());
                } catch (IOException e) {
                    logger.error("error delete bucket {}", routingBucket.index());
                }
            }
        }
    }

    private Bucket getOrCreate(int index) {
        Bucket bucket = bucket(index);
        if (bucket == null) {
            bucket = bucketFactory.create(index);
            buckets = Immutable.compose(buckets, index, bucket);
        }
        return bucket;
    }

    private void deleteIfExists(int index) throws IOException {
        if (contains(index)) {
            logger.info("delete bucket {}", index);
            Bucket bucket = bucket(index);
            buckets = Immutable.subtract(buckets, index);
            bucket.close();
            bucket.delete();
        }
    }
}
