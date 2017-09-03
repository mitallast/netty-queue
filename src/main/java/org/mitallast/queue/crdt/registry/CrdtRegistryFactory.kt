package org.mitallast.queue.crdt.registry

import org.mitallast.queue.crdt.replication.Replicator

interface CrdtRegistryFactory {
    fun create(index: Int, replica: Long, replicator: Replicator): CrdtRegistry
}
