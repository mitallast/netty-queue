package org.mitallast.queue.crdt.routing.fsm

import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.transport.DiscoveryNode

data class AddReplica(val bucket: Int, val member: DiscoveryNode) : Message {
    companion object {
        val codec = Codec.of(
            ::AddReplica,
            AddReplica::bucket,
            AddReplica::member,
            Codec.intCodec(),
            DiscoveryNode.codec
        )
    }
}
