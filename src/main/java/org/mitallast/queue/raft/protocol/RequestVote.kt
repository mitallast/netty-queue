package org.mitallast.queue.raft.protocol

import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.transport.DiscoveryNode

data class RequestVote(
    val term: Long,
    val candidate: DiscoveryNode,
    val lastLogTerm: Long,
    val lastLogIndex: Long) : Message {

    companion object {
        val codec = Codec.of(
            ::RequestVote,
            RequestVote::term,
            RequestVote::candidate,
            RequestVote::lastLogTerm,
            RequestVote::lastLogIndex,
            Codec.longCodec(),
            DiscoveryNode.codec,
            Codec.longCodec(),
            Codec.longCodec()
        )
    }
}
