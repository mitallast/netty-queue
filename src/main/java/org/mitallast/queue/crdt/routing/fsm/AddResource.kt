package org.mitallast.queue.crdt.routing.fsm

import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.crdt.routing.ResourceType

data class AddResource(val id: Long, val type: ResourceType) : Message {
    companion object {
        val codec = Codec.of(
            ::AddResource,
            AddResource::id,
            AddResource::type,
            Codec.longCodec(),
            Codec.enumCodec(ResourceType::class.java)
        )
    }
}
