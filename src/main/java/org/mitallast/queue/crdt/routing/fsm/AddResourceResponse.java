package org.mitallast.queue.crdt.routing.fsm;

import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.crdt.routing.ResourceType;

public class AddResourceResponse implements Message {
    public static final Codec<AddResourceResponse> codec = Codec.of(
        AddResourceResponse::new,
        AddResourceResponse::type,
        AddResourceResponse::id,
        AddResourceResponse::isCreated,
        Codec.enumCodec(ResourceType.class),
        Codec.longCodec,
        Codec.booleanCodec
    );

    private final ResourceType type;
    private final long id;
    private final boolean created;

    public AddResourceResponse(ResourceType type, long id, boolean created) {
        this.type = type;
        this.id = id;
        this.created = created;
    }

    public ResourceType type() {
        return type;
    }

    public long id() {
        return id;
    }

    public boolean isCreated() {
        return created;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AddResourceResponse that = (AddResourceResponse) o;

        if (id != that.id) return false;
        if (created != that.created) return false;
        return type == that.type;
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + (int) (id ^ (id >>> 32));
        result = 31 * result + (created ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "AddResourceResponse{" +
            "type=" + type +
            ", id=" + id +
            ", created=" + created +
            '}';
    }
}
