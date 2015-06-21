package org.mitallast.queue.action.queue.get;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.queue.QueueMessage;

import java.io.IOException;

public class GetResponse implements ActionResponse<GetResponse.Builder, GetResponse> {

    private final QueueMessage message;

    private GetResponse(QueueMessage message) {
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

    public static class Builder implements EntryBuilder<Builder, GetResponse> {
        private QueueMessage message;

        @Override
        public Builder from(GetResponse entry) {
            message = entry.message;
            return this;
        }

        public Builder setMessage(QueueMessage message) {
            this.message = message;
            return this;
        }

        @Override
        public GetResponse build() {
            return new GetResponse(message);
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