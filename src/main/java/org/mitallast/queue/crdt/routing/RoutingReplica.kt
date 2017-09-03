package org.mitallast.queue.crdt.routing

import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.transport.DiscoveryNode

class RoutingReplica @JvmOverloads constructor(
    val id: Long,
    val member: DiscoveryNode,
    val state: State = State.OPENED) : Message {

    enum class State {
        OPENED, CLOSED
    }

    val isOpened: Boolean
        get() = state == State.OPENED

    val isClosed: Boolean
        get() = state == State.CLOSED

    fun close(): RoutingReplica {
        return RoutingReplica(id, member, State.CLOSED)
    }

    companion object {
        val codec = Codec.of(
            ::RoutingReplica,
            RoutingReplica::id,
            RoutingReplica::member,
            RoutingReplica::state,
            Codec.longCodec(),
            DiscoveryNode.codec,
            Codec.enumCodec(State::class.java)
        )
    }
}
