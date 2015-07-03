package org.mitallast.queue.action.queue.delete;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.validation.ValidationBuilder;

import java.io.IOException;
import java.util.UUID;

public class DeleteRequest implements ActionRequest<DeleteRequest> {

    private final String queue;
    private final UUID messageUUID;

    private DeleteRequest(String queue, UUID messageUUID) {
        this.queue = queue;
        this.messageUUID = messageUUID;
    }

    public String queue() {
        return queue;
    }

    public UUID messageUUID() {
        return messageUUID;
    }

    @Override
    public ValidationBuilder validate() {
        return ValidationBuilder.builder()
            .missing("queue", queue)
            .missing("messageUUID", messageUUID);
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<DeleteRequest> {
        private String queue;
        private UUID messageUUID;

        private Builder from(DeleteRequest entry) {
            queue = entry.queue;
            messageUUID = entry.messageUUID;
            return this;
        }

        public Builder setQueue(String queue) {
            this.queue = queue;
            return this;
        }

        public Builder setMessageUUID(UUID messageUUID) {
            this.messageUUID = messageUUID;
            return this;
        }

        @Override
        public DeleteRequest build() {
            return new DeleteRequest(queue, messageUUID);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            queue = stream.readTextOrNull();
            messageUUID = stream.readUUIDOrNull();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeTextOrNull(queue);
            stream.writeUUIDOrNull(messageUUID);
        }
    }
}