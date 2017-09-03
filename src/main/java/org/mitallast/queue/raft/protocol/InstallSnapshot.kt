package org.mitallast.queue.raft.protocol

import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.transport.DiscoveryNode

data class InstallSnapshot(val leader: DiscoveryNode, val term: Long, val snapshot: RaftSnapshot) : Message {
    companion object {
        val codec = Codec.of(
            ::InstallSnapshot,
            InstallSnapshot::leader,
            InstallSnapshot::term,
            InstallSnapshot::snapshot,
            DiscoveryNode.codec,
            Codec.longCodec(),
            RaftSnapshot.codec
        )
    }
}
