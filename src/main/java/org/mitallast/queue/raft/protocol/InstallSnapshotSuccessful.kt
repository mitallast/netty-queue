package org.mitallast.queue.raft.protocol

import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.transport.DiscoveryNode

data class InstallSnapshotSuccessful(val member: DiscoveryNode, val term: Long, val lastIndex: Long) : Message {
    companion object {
        val codec = Codec.of(
            ::InstallSnapshotSuccessful,
            InstallSnapshotSuccessful::member,
            InstallSnapshotSuccessful::term,
            InstallSnapshotSuccessful::lastIndex,
            DiscoveryNode.codec,
            Codec.longCodec(),
            Codec.longCodec()
        )
    }
}
