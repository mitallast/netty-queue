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
}
