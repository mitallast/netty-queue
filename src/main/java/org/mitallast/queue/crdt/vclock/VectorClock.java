package org.mitallast.queue.crdt.vclock;

import java.io.Closeable;

public interface VectorClock extends Closeable {

    void put(long replica, long vclock);

    long get(long replica);

    void delete();

    @Override
    void close();
}
