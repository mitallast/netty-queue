package org.mitallast.queue.crdt.registry;

import org.mitallast.queue.crdt.replication.Replicator;

public interface CrdtRegistryFactory {
    CrdtRegistry create(int index, long replica, Replicator replicator);
}
