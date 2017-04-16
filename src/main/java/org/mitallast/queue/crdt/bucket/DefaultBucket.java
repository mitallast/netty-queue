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
import org.mitallast.queue.crdt.vclock.VectorClock;
import org.mitallast.queue.crdt.vclock.VectorClockFactory;

import javax.inject.Inject;

public class DefaultBucket implements Bucket {
    private final Logger logger;
    private final int index;
    private final CrdtRegistry registry;
    private final ReplicatedLog log;
    private final VectorClock vclock;
    private final Replicator replicator;

    @Inject
    public DefaultBucket(
        @Assisted int index,
        CrdtRegistryFactory crdtRegistryFactory,
        ReplicatedLogFactory logFactory,
        VectorClockFactory vclockFactory,
        ReplicatorFactory replicatorFactory
    ) {
        this.index = index;
        logger = LogManager.getLogger("replicator[" + index + "]");
        vclock = vclockFactory.create(index);
        replicator = replicatorFactory.create(this);
        registry = crdtRegistryFactory.create(index, replicator);
        log = logFactory.create(index, new DefaultCompactionFilter(registry));

        replicator.start();
    }

    @Override
    public int index() {
        return index;
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
    public VectorClock vclock() {
        return vclock;
    }

    @Override
    public void close() {
        logger.info("close");
        replicator.stop();
        log.close();
        vclock.close();
    }

    @Override
    public void delete() {
        logger.info("delete");
        log.delete();
        vclock.delete();
    }
}
