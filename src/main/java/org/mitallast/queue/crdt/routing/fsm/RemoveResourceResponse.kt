package org.mitallast.queue.crdt.routing.fsm

import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.crdt.routing.ResourceType

data class RemoveResourceResponse(val type: ResourceType, val id: Long, val isRemoved: Boolean) : Message {

    companion object {
        val codec = Codec.of(
            ::RemoveResourceResponse,
            RemoveResourceResponse::type,
            RemoveResourceResponse::id,
            RemoveResourceResponse::isRemoved,
            Codec.enumCodec(ResourceType::class.java),
            Codec.longCodec(),
            Codec.booleanCodec()
        )
    }
}
