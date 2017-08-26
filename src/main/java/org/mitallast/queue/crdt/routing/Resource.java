package org.mitallast.queue.crdt.routing;

import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;

public class Resource implements Message {
    public static final Codec<Resource> codec = Codec.of(
        Resource::new,
        Resource::id,
        Resource::type,
        Codec.longCodec,
        Codec.enumCodec(ResourceType.class)
    );

    private final long id;
    private final ResourceType type;

    public Resource(long id, ResourceType type) {
        this.id = id;
        this.type = type;
    }

    public long id() {
        return id;
    }

    public ResourceType type() {
        return type;
    }
}
