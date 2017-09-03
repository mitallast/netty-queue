package org.mitallast.queue.crdt.routing

import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message

enum class ResourceType {
    LWWRegister, GCounter, GSet, OrderedGSet
}

class Resource(val id: Long, val type: ResourceType) : Message {
    companion object {
        val codec = Codec.of(
            ::Resource,
            Resource::id,
            Resource::type,
            Codec.longCodec(),
            Codec.enumCodec(ResourceType::class.java)
        )
    }
}
