package org.mitallast.queue.raft.protocol

import javaslang.collection.Vector
import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.transport.DiscoveryNode

data class AppendEntries(
    val member: DiscoveryNode,
    val term: Long,
    val prevLogTerm: Long,
    val prevLogIndex: Long,
    val leaderCommit: Long,
    val entries: Vector<LogEntry>) : Message {

    companion object {
        val codec = Codec.of(
            ::AppendEntries,
            AppendEntries::member,
            AppendEntries::term,
            AppendEntries::prevLogTerm,
            AppendEntries::prevLogIndex,
            AppendEntries::leaderCommit,
            AppendEntries::entries,
            DiscoveryNode.codec,
            Codec.longCodec(),
            Codec.longCodec(),
            Codec.longCodec(),
            Codec.longCodec(),
            Codec.vectorCodec(LogEntry.codec)
        )
    }
}
