package org.mitallast.queue.crdt.routing.fsm

import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.crdt.routing.ResourceType

data class AddResourceResponse(val type: ResourceType, val id: Long, val isCreated: Boolean) : Message {
    companion object {
        val codec = Codec.of(
            ::AddResourceResponse,
            AddResourceResponse::type,
            AddResourceResponse::id,
            AddResourceResponse::isCreated,
            Codec.enumCodec(ResourceType::class.java),
            Codec.longCodec(),
            Codec.booleanCodec()
        )
    }
}
