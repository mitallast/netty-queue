package org.mitallast.queue.action.queue.push;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;
import java.util.UUID;

public class PushResponse extends ActionResponse implements Streamable {

    private UUID uuid;

    public PushResponse() {
    }

    public PushResponse(UUID uuid) {
        this.uuid = uuid;
    }

    public UUID getUUID() {
        return uuid;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        uuid = stream.readUUID();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeUUID(uuid);
    }
}
