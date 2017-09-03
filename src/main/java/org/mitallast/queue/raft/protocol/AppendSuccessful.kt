package org.mitallast.queue.raft.protocol

import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.transport.DiscoveryNode

data class AppendSuccessful(val member: DiscoveryNode, val term: Long, val lastIndex: Long) : Message {
    companion object {
        val codec = Codec.of(
            ::AppendSuccessful,
            AppendSuccessful::member,
            AppendSuccessful::term,
            AppendSuccessful::lastIndex,
            DiscoveryNode.codec,
            Codec.longCodec(),
            Codec.longCodec()
        )
    }
}
