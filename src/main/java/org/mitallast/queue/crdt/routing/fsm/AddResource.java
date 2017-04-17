package org.mitallast.queue.crdt.routing.fsm;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.crdt.routing.ResourceType;

public class AddResource implements Streamable {
    private final long id;
    private final ResourceType type;

    public AddResource(long id, ResourceType type) {
        this.id = id;
        this.type = type;
    }

    public AddResource(StreamInput stream) {
        id = stream.readLong();
        type = stream.readEnum(ResourceType.class);
    }

    @Override
    public void writeTo(StreamOutput stream) {
        stream.writeLong(id);
        stream.writeEnum(type);
    }

    public long id() {
        return id;
    }

    public ResourceType type() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AddResource that = (AddResource) o;

        if (id != that.id) return false;
        return type == that.type;
    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + type.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "AddResource{" +
            "id=" + id +
            ", type=" + type +
            '}';
    }
}
