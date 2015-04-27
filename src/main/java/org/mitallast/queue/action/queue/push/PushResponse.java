package org.mitallast.queue.action.queue.push;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;
import java.util.UUID;

public class PushResponse extends ActionResponse implements Streamable {

    private UUID messageUUID;

    public PushResponse() {
    }

    public PushResponse(UUID messageUUID) {
        this.messageUUID = messageUUID;
    }

    public UUID getMessageUUID() {
        return messageUUID;
    }

    public void setMessageUUID(UUID messageUUID) {
        this.messageUUID = messageUUID;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        messageUUID = stream.readUUID();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeUUID(messageUUID);
    }
}
