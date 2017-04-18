package org.mitallast.queue.crdt.bucket;

import org.mitallast.queue.crdt.log.ReplicatedLog;
import org.mitallast.queue.crdt.registry.CrdtRegistry;
import org.mitallast.queue.crdt.replication.Replicator;
import org.mitallast.queue.crdt.vclock.VectorClock;

import java.io.Closeable;

public interface Bucket extends Closeable {

    int index();

    long replica();

    CrdtRegistry registry();

    ReplicatedLog log();

    Replicator replicator();

    VectorClock vclock();

    void delete();

    @Override
    void close();
}
