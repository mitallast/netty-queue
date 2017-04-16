package org.mitallast.queue.crdt.routing.fsm;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.crdt.routing.ResourceType;

public class RemoveResourceResponse implements Streamable {
    private final ResourceType type;
    private final long id;
    private final boolean removed;

    public RemoveResourceResponse(ResourceType type, long id, boolean removed) {
        this.type = type;
        this.id = id;
        this.removed = removed;
    }

    public RemoveResourceResponse(StreamInput stream) {
        type = stream.readEnum(ResourceType.class);
        id = stream.readLong();
        removed = stream.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput stream) {
        stream.writeEnum(type);
        stream.writeLong(id);
        stream.writeBoolean(removed);
    }

    public ResourceType type() {
        return type;
    }

    public long id() {
        return id;
    }

    public boolean isRemoved() {
        return removed;
    }
}
