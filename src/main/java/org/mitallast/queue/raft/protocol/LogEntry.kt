package org.mitallast.queue.raft.protocol

import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message

data class LogEntry(val term: Long, val index: Long, val session: Long, val command: Message) : Message {

    companion object {
        val codec = Codec.of(
            ::LogEntry,
            LogEntry::term,
            LogEntry::index,
            LogEntry::session,
            LogEntry::command,
            Codec.longCodec(),
            Codec.longCodec(),
            Codec.longCodec(),
            Codec.anyCodec()
        )
    }
}
