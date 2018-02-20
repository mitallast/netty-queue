package org.mitallast.queue.raft.resource

import io.vavr.collection.HashMap
import io.vavr.collection.Map
import io.vavr.collection.Vector
import io.vavr.control.Option
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.raft.protocol.RaftSnapshot
import org.mitallast.queue.raft.protocol.RaftSnapshotMetadata


@Suppress("UNCHECKED_CAST")
class ResourceRegistry {
    @Volatile private var resources = Vector.empty<ResourceFSM>()
    @Volatile private var handlers: Map<Class<*>, (Long, event: Message) -> Option<Message>> = HashMap.empty()

    @Synchronized
    fun register(fsm: ResourceFSM) {
        resources = resources.append(fsm)
    }

    @Synchronized
    fun <T : Message> register(type: Class<T>, handler: (Long, event: T) -> Option<Message>) {
        handlers = handlers.put(type, handler as (Long, event: Message) -> Option<Message>)
    }

    fun <T : Message> apply(index: Long, event: T): Option<Message> {
        return handlers.get(event.javaClass)
            .map { it as (Long, event: T) -> Option<Message> }
            .flatMap { it.invoke(index, event) }
    }

    fun prepareSnapshot(snapshotMeta: RaftSnapshotMetadata): RaftSnapshot {
        val snapshots = resources.flatMap { r -> r.prepareSnapshot(snapshotMeta) }
        return RaftSnapshot(snapshotMeta, snapshots)
    }
}
