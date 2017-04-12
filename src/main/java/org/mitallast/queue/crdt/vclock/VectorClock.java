package org.mitallast.queue.crdt.vclock;

import gnu.trove.map.TObjectLongMap;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.Closeable;
import java.io.IOException;

public interface VectorClock extends Closeable {

    void put(DiscoveryNode node, long vclock) throws IOException;

    long get(DiscoveryNode node);

    TObjectLongMap<DiscoveryNode> getAll();

    void delete() throws IOException;
}
