package org.mitallast.queue.raft.protocol

import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.transport.DiscoveryNode

data class AddServer(val member: DiscoveryNode) : Message {
    companion object {
        val codec = Codec.of(
            ::AddServer,
            AddServer::member,
            DiscoveryNode.codec
        )
    }
}