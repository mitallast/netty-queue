package org.mitallast.queue.action.queue.peek;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.queue.QueueMessage;

import java.io.IOException;

public class PeekQueueResponse implements ActionResponse<PeekQueueResponse.Builder, PeekQueueResponse> {

    private final QueueMessage message;

    private PeekQueueResponse(QueueMessage message) {
        this.message = message;
    }

    public QueueMessage message() {
        return message;
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<Builder, PeekQueueResponse> {
        private QueueMessage message;

        @Override
        public Builder from(PeekQueueResponse entry) {
            message = entry.message;
            return this;
        }

        public Builder setMessage(QueueMessage message) {
            this.message = message;
            return this;
        }

        @Override
        public PeekQueueResponse build() {
            return new PeekQueueResponse(message);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            message = stream.readStreamableOrNull(QueueMessage::new);
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeStreamableOrNull(message);
        }
    }
}