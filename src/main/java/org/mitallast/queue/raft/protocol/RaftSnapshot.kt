package org.mitallast.queue.raft.protocol

import javaslang.collection.Vector
import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message

data class RaftSnapshot(val meta: RaftSnapshotMetadata, val data: Vector<Message>) : Message {

    fun toEntry(): LogEntry {
        return LogEntry(meta.lastIncludedTerm, meta.lastIncludedIndex, 0, this)
    }

    companion object {
        val codec = Codec.of(
            ::RaftSnapshot,
            RaftSnapshot::meta,
            RaftSnapshot::data,
            RaftSnapshotMetadata.codec,
            Codec.vectorCodec(Codec.anyCodec())
        )
    }
}
