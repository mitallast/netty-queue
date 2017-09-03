package org.mitallast.queue.raft.protocol

import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.transport.DiscoveryNode

data class DeclineCandidate(val member: DiscoveryNode, val term: Long) : Message {
    companion object {
        val codec = Codec.of(
            ::DeclineCandidate,
            DeclineCandidate::member,
            DeclineCandidate::term,
            DiscoveryNode.codec,
            Codec.longCodec()
        )
    }
}
