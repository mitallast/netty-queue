package org.mitallast.queue.raft.protocol

import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.raft.cluster.ClusterConfiguration

data class RaftSnapshotMetadata(
    val lastIncludedTerm: Long,
    val lastIncludedIndex: Long,
    val config: ClusterConfiguration) : Message {

    companion object {
        val codec = Codec.of(
            ::RaftSnapshotMetadata,
            RaftSnapshotMetadata::lastIncludedTerm,
            RaftSnapshotMetadata::lastIncludedIndex,
            RaftSnapshotMetadata::config,
            Codec.longCodec(),
            Codec.longCodec(),
            ClusterConfiguration.codec
        )
    }
}
