package org.mitallast.queue.crdt.replication.state

interface ReplicaStateFactory {
    fun create(index: Int, replica: Long): ReplicaState
}
