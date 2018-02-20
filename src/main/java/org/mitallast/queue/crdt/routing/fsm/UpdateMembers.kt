package org.mitallast.queue.crdt.routing.fsm

import io.vavr.collection.Set
import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.transport.DiscoveryNode


data class UpdateMembers(val members: Set<DiscoveryNode>) : Message {

    companion object {
        val codec = Codec.of(
            ::UpdateMembers,
            UpdateMembers::members,
            Codec.setCodec(DiscoveryNode.codec)
        )
    }
}
