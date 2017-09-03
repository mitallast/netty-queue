package org.mitallast.queue.raft.protocol

import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.transport.DiscoveryNode

data class InstallSnapshotRejected(val member: DiscoveryNode, val term: Long) : Message {
    companion object {
        val codec = Codec.of(
            ::InstallSnapshotRejected,
            InstallSnapshotRejected::member,
            InstallSnapshotRejected::term,
            DiscoveryNode.codec,
            Codec.longCodec()
        )
    }
}
