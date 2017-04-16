package org.mitallast.queue.crdt.vclock;

import org.mitallast.queue.transport.DiscoveryNode;

import java.io.Closeable;

public interface VectorClock extends Closeable {

    void put(DiscoveryNode node, long vclock);

    long get(DiscoveryNode node);

    void delete();

    @Override
    void close();
}
