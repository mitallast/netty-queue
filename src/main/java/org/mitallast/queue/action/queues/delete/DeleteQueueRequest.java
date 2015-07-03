package org.mitallast.queue.action.queues.delete;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.validation.ValidationBuilder;

import java.io.IOException;

public class DeleteQueueRequest implements ActionRequest<DeleteQueueRequest> {
    private final String queue;
    private final String reason;

    private DeleteQueueRequest(String queue, String reason) {
        this.queue = queue;
        this.reason = reason;
    }

    public String queue() {
        return queue;
    }

    public String reason() {
        return reason;
    }

    @Override
    public ValidationBuilder validate() {
        return ValidationBuilder.builder()
            .missing("queue", queue)
            .missing("reason", reason);
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<DeleteQueueRequest> {
        private String queue;
        private String reason;

        private Builder from(DeleteQueueRequest entry) {
            queue = entry.queue;
            reason = entry.reason;
            return this;
        }

        public Builder setQueue(String queue) {
            this.queue = queue;
            return this;
        }

        public Builder setReason(String reason) {
            this.reason = reason;
            return this;
        }

        @Override
        public DeleteQueueRequest build() {
            return new DeleteQueueRequest(queue, reason);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            queue = stream.readText();
            reason = stream.readText();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeText(queue);
            stream.writeText(reason);
        }
    }
}
