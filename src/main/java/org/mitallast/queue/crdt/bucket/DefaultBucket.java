package org.mitallast.queue.crdt.bucket;

import com.google.inject.assistedinject.Assisted;
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
import java.io.IOException;

public class DefaultBucket implements Bucket {
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
    ) throws IOException {
        this.index = index;
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
    public void close() throws IOException {
        replicator.stop();
        log.close();
        vclock.close();
    }

    @Override
    public void delete() throws IOException {
        log.delete();
        vclock.delete();
    }
}
