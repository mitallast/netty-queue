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
}
