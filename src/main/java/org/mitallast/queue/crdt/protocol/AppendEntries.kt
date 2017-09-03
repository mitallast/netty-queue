package org.mitallast.queue.crdt.protocol

import javaslang.collection.Vector
import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.crdt.log.LogEntry

data class AppendEntries(
    val bucket: Int,
    val replica: Long,
    val prevIndex: Long,
    val entries: Vector<LogEntry>) : Message {

    companion object {
        val codec = Codec.of(
            ::AppendEntries,
            AppendEntries::bucket,
            AppendEntries::replica,
            AppendEntries::prevIndex,
            AppendEntries::entries,
            Codec.intCodec(),
            Codec.longCodec(),
            Codec.longCodec(),
            Codec.vectorCodec(LogEntry.codec)
        )
    }
}
