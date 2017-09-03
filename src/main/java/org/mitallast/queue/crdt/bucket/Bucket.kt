package org.mitallast.queue.crdt.bucket

import org.mitallast.queue.crdt.log.ReplicatedLog
import org.mitallast.queue.crdt.registry.CrdtRegistry
import org.mitallast.queue.crdt.replication.Replicator
import org.mitallast.queue.crdt.replication.state.ReplicaState

import java.io.Closeable
import java.util.concurrent.locks.ReentrantLock

interface Bucket : Closeable {

    fun index(): Int

    fun replica(): Long

    fun lock(): ReentrantLock

    fun registry(): CrdtRegistry

    fun log(): ReplicatedLog

    fun replicator(): Replicator

    fun state(): ReplicaState

    fun delete()

    override fun close()
}
