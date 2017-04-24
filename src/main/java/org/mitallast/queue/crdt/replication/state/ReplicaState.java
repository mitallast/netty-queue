package org.mitallast.queue.crdt.replication.state;

import java.io.Closeable;

public interface ReplicaState extends Closeable {

    void put(long replica, long logIndex);

    long get(long replica);

    void delete();

    @Override
    void close();
}
