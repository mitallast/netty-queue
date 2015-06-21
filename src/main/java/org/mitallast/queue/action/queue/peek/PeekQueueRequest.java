package org.mitallast.queue.action.queue.peek;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.validation.ValidationBuilder;

import java.io.IOException;

public class PeekQueueRequest implements ActionRequest<PeekQueueRequest.Builder, PeekQueueRequest> {

    private final String queue;

    private PeekQueueRequest(String queue) {
        this.queue = queue;
    }

    public String queue() {
        return queue;
    }

    @Override
    public ValidationBuilder validate() {
        return ValidationBuilder.builder()
            .missing("queue", queue);
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<Builder, PeekQueueRequest> {
        private String queue;

        @Override
        public Builder from(PeekQueueRequest entry) {
            queue = entry.queue;
            return this;
        }

        public Builder setQueue(String queue) {
            this.queue = queue;
            return this;
        }

        @Override
        public PeekQueueRequest build() {
            return new PeekQueueRequest(queue);
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeText(queue);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            queue = stream.readText();
        }
    }
}