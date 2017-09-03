package org.mitallast.queue.crdt.routing.fsm

import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.crdt.routing.ResourceType

data class RemoveResource(val type: ResourceType, val id: Long) : Message {
    companion object {
        val codec = Codec.of(
            ::RemoveResource,
            RemoveResource::type,
            RemoveResource::id,
            Codec.enumCodec(ResourceType::class.java),
            Codec.longCodec()
        )
    }
}
