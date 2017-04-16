package org.mitallast.queue.crdt.routing;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

public class Resource implements Streamable {
    private final long id;
    private final ResourceType type;

    public Resource(long id, ResourceType type) {
        this.id = id;
        this.type = type;
    }

    public Resource(StreamInput stream) {
        this.id = stream.readLong();
        this.type = stream.readEnum(ResourceType.class);
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
}
