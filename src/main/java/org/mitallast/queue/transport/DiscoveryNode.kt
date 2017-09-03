package org.mitallast.queue.transport

import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message

data class DiscoveryNode(val host: String, val port: Int) : Message {
    companion object {
        val codec = Codec.of(
            ::DiscoveryNode,
            DiscoveryNode::host,
            DiscoveryNode::port,
            Codec.stringCodec(),
            Codec.intCodec()
        )
    }
}
