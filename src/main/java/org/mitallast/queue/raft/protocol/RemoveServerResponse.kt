package org.mitallast.queue.raft.protocol

import javaslang.control.Option
import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.transport.DiscoveryNode

data class RemoveServerResponse(val status: Status, val leader: Option<DiscoveryNode>) : Message {

    enum class Status {
        OK, TIMEOUT, NOT_LEADER
    }

    companion object {
        val codec = Codec.of(
            ::RemoveServerResponse,
            RemoveServerResponse::status,
            RemoveServerResponse::leader,
            Codec.enumCodec(Status::class.java),
            Codec.optionCodec(DiscoveryNode.codec)
        )
    }
}
