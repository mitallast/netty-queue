package org.mitallast.queue.crdt.log

import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message

data class LogEntry(val index: Long, val id: Long, val event: Message) : Message {
    companion object {
        val codec = Codec.of(
            ::LogEntry,
            LogEntry::index,
            LogEntry::id,
            LogEntry::event,
            Codec.longCodec(),
            Codec.longCodec(),
            Codec.anyCodec()
        )
    }
}
