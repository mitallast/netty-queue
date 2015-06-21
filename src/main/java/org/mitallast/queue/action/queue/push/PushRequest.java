package org.mitallast.queue.action.queue.push;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.validation.ValidationBuilder;
import org.mitallast.queue.queue.QueueMessage;

import java.io.IOException;

public class PushRequest implements ActionRequest<PushRequest.Builder, PushRequest> {
    private final String queue;
    private final QueueMessage message;

    private PushRequest(String queue, QueueMessage message) {
        this.queue = queue;
        this.message = message;
    }

    public String queue() {
        return queue;
    }

    public QueueMessage message() {
        return message;
    }

    @Override
    public ValidationBuilder validate() {
        return ValidationBuilder.builder()
            .missing("queue", queue)
            .missing("message", message);
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<Builder, PushRequest> {
        private String queue;
        private QueueMessage message;

        @Override
        public Builder from(PushRequest entry) {
            queue = entry.queue;
            message = entry.message;
            return this;
        }

        public Builder setQueue(String queue) {
            this.queue = queue;
            return this;
        }

        public Builder setMessage(QueueMessage message) {
            this.message = message;
            return this;
        }

        @Override
        public PushRequest build() {
            return new PushRequest(queue, message);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            queue = stream.readTextOrNull();
            message = stream.readStreamableOrNull(QueueMessage::new);
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeTextOrNull(queue);
            stream.writeStreamableOrNull(message);
        }
    }

}
