package org.mitallast.queue.action.queue.get;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.validation.ValidationBuilder;

import java.io.IOException;
import java.util.UUID;

public class GetRequest implements ActionRequest<GetRequest> {

    private final String queue;
    private final UUID uuid;

    private GetRequest(String queue, UUID uuid) {
        this.queue = queue;
        this.uuid = uuid;
    }

    public String queue() {
        return queue;
    }

    public UUID uuid() {
        return uuid;
    }

    @Override
    public ValidationBuilder validate() {
        return ValidationBuilder.builder()
            .missing("queue", queue)
            .missing("uuid", uuid);
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<GetRequest> {
        private String queue;
        private UUID uuid;

        private Builder from(GetRequest entry) {
            queue = entry.queue;
            uuid = entry.uuid;
            return this;
        }

        public Builder setQueue(String queue) {
            this.queue = queue;
            return this;
        }

        public Builder setUuid(UUID uuid) {
            this.uuid = uuid;
            return this;
        }

        @Override
        public GetRequest build() {
            return new GetRequest(queue, uuid);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            queue = stream.readText();
            uuid = stream.readUUID();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeText(queue);
            stream.writeUUID(uuid);
        }
    }
}