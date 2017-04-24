package org.mitallast.queue.crdt.bucket;

import com.google.inject.assistedinject.Assisted;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.crdt.log.DefaultCompactionFilter;
import org.mitallast.queue.crdt.log.ReplicatedLog;
import org.mitallast.queue.crdt.log.ReplicatedLogFactory;
import org.mitallast.queue.crdt.registry.CrdtRegistry;
import org.mitallast.queue.crdt.registry.CrdtRegistryFactory;
import org.mitallast.queue.crdt.replication.Replicator;
import org.mitallast.queue.crdt.replication.ReplicatorFactory;
import org.mitallast.queue.crdt.replication.state.ReplicaState;
import org.mitallast.queue.crdt.replication.state.ReplicaStateFactory;

import javax.inject.Inject;
import java.util.concurrent.locks.ReentrantLock;

public class DefaultBucket implements Bucket {
    private final Logger logger;
    private final int index;
    private final long replica;
    private final ReentrantLock lock;
    private final CrdtRegistry registry;
    private final ReplicatedLog log;
    private final ReplicaState replicaState;
    private final Replicator replicator;

    @Inject
    public DefaultBucket(
        @Assisted int index,
        @Assisted long replica,
        CrdtRegistryFactory crdtRegistryFactory,
        ReplicatedLogFactory logFactory,
        ReplicaStateFactory stateFactory,
        ReplicatorFactory replicatorFactory
    ) {
        this.index = index;
        this.replica = replica;
        logger = LogManager.getLogger("replicator[" + index + "]");
        lock = new ReentrantLock();
        replicaState = stateFactory.create(index, replica);
        replicator = replicatorFactory.create(this);
        registry = crdtRegistryFactory.create(index, replica, replicator);
        log = logFactory.create(index, replica, new DefaultCompactionFilter(registry));

        replicator.start();
    }

    @Override
    public int index() {
        return index;
    }

    @Override
    public long replica() {
        return replica;
    }

    @Override
    public ReentrantLock lock() {
        return lock;
    }

    @Override
    public CrdtRegistry registry() {
        return registry;
    }

    @Override
    public ReplicatedLog log() {
        return log;
    }

    @Override
    public Replicator replicator() {
        return replicator;
    }

    @Override
    public ReplicaState state() {
        return replicaState;
    }

    @Override
    public void close() {
        logger.info("close");
        replicator.stop();
        log.close();
        replicaState.close();
    }

    @Override
    public void delete() {
        logger.info("delete");
        log.delete();
        replicaState.delete();
    }
}
