package org.mitallast.queue.crdt.replication.state

import java.io.Closeable

interface ReplicaState : Closeable {

    fun put(replica: Long, logIndex: Long)

    operator fun get(replica: Long): Long

    fun delete()

    override fun close()
}
