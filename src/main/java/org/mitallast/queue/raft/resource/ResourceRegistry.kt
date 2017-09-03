package org.mitallast.queue.raft.resource

import javaslang.collection.HashMap
import javaslang.collection.Map
import javaslang.collection.Vector
import javaslang.control.Option
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.raft.protocol.RaftSnapshot
import org.mitallast.queue.raft.protocol.RaftSnapshotMetadata


@Suppress("UNCHECKED_CAST")
class ResourceRegistry {
    @Volatile private var resources = Vector.empty<ResourceFSM>()
    @Volatile private var handlers: Map<Class<*>, ResourceHandler<*>> = HashMap.empty()

    @Synchronized
    fun register(fsm: ResourceFSM) {
        resources = resources.append(fsm)
    }

    @Synchronized
    fun <T : Message> register(type: Class<T>, handler: ResourceHandler<T>) {
        handlers = handlers.put(type, handler)
    }

    fun <T : Message> apply(index: Long, event: T): Option<Message> {
        return handlers.get(event.javaClass)
            .map { (it as ResourceHandler<T>).apply(index, event) }
    }

    fun prepareSnapshot(snapshotMeta: RaftSnapshotMetadata): RaftSnapshot {
        val snapshots = resources.flatMap { r -> r.prepareSnapshot(snapshotMeta) }
        return RaftSnapshot(snapshotMeta, snapshots)
    }

    @FunctionalInterface
    interface ResourceHandler<in T : Message> {
        fun apply(index: Long, event: T): Message
    }
}
