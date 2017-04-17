package org.mitallast.queue.crdt.routing.fsm;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.crdt.routing.ResourceType;

public class RemoveResource implements Streamable {
    private final ResourceType type;
    private final long id;

    public RemoveResource(ResourceType type, long id) {
        this.type = type;
        this.id = id;
    }

    public RemoveResource(StreamInput stream) {
        type = stream.readEnum(ResourceType.class);
        id = stream.readLong();
    }

    @Override
    public void writeTo(StreamOutput stream) {
        stream.writeEnum(type);
        stream.writeLong(id);
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
