package org.mitallast.queue.crdt.routing.fsm;

import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.crdt.routing.ResourceType;

public class RemoveResource implements Message {
    public static final Codec<RemoveResource> codec = Codec.of(
        RemoveResource::new,
        RemoveResource::type,
        RemoveResource::id,
        Codec.enumCodec(ResourceType.class),
        Codec.longCodec
    );

    private final ResourceType type;
    private final long id;

    public RemoveResource(ResourceType type, long id) {
        this.type = type;
        this.id = id;
    }

    public ResourceType type() {
        return type;
    }

    public long id() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RemoveResource that = (RemoveResource) o;

        if (id != that.id) return false;
        return type == that.type;
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + (int) (id ^ (id >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "RemoveResource{" +
            "type=" + type +
            ", id=" + id +
            '}';
    }
}
