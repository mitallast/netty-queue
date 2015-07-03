package org.mitallast.queue.action.queue.push;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;

import java.io.IOException;
import java.util.UUID;

public class PushResponse implements ActionResponse<PushResponse> {
    private final UUID messageUUID;

    private PushResponse(UUID messageUUID) {
        this.messageUUID = messageUUID;
    }

    public UUID messageUUID() {
        return messageUUID;
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<PushResponse> {
        private UUID messageUUID;

        private Builder from(PushResponse entry) {
            messageUUID = entry.messageUUID;
            return this;
        }

        public Builder setMessageUUID(UUID messageUUID) {
            this.messageUUID = messageUUID;
            return this;
        }

        @Override
        public PushResponse build() {
            return new PushResponse(messageUUID);
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
}
