package org.mitallast.queue.crdt.routing.fsm;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.crdt.routing.ResourceType;

import java.io.IOException;

public class AddResourceResponse implements Streamable {
    private final ResourceType type;
    private final long id;
    private final boolean created;

    public AddResourceResponse(ResourceType type, long id, boolean created) {
        this.type = type;
        this.id = id;
        this.created = created;
    }

    public AddResourceResponse(StreamInput stream) throws IOException {
        type = stream.readEnum(ResourceType.class);
        id = stream.readLong();
        created = stream.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeEnum(type);
        stream.writeLong(id);
        stream.writeBoolean(created);
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
}
