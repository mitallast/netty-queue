package org.mitallast.queue.crdt.routing.fsm;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.crdt.routing.ResourceType;


import java.io.IOException;

public class AddResource implements Streamable {
    private final ResourceType type;
    private final long id;
    private final int replicas;

    public AddResource(ResourceType type, long id, int replicas) {
        this.type = type;
        this.id = id;
        this.replicas = replicas;
    }

    public AddResource(StreamInput stream) throws IOException {
        type = stream.readEnum(ResourceType.class);
        id = stream.readLong();
        replicas = stream.readInt();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeEnum(type);
        stream.writeLong(id);
        stream.writeInt(replicas);
    }

    public ResourceType type() {
        return type;
    }

    public long id() {
        return id;
    }

    public int replicas() {
        return replicas;
    }
}
