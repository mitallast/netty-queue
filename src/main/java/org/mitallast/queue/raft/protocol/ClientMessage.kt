package org.mitallast.queue.raft.protocol

import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message

data class ClientMessage(val command: Message, val session: Long) : Message {
    companion object {
        val codec = Codec.of<ClientMessage, Message, Long>(
            ::ClientMessage,
            ClientMessage::command,
            ClientMessage::session,
            Codec.anyCodec(),
            Codec.longCodec()
        )
    }
}
