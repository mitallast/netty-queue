package org.mitallast.queue.crdt.replication.state;

public interface ReplicaStateFactory {
    ReplicaState create(int index, long replica);
}
