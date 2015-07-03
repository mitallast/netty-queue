package org.mitallast.queue.action.queues.create;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;

import java.io.IOException;

public class CreateQueueResponse implements ActionResponse<CreateQueueResponse> {

    private CreateQueueResponse() {
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<CreateQueueResponse> {

        private Builder from(CreateQueueResponse entry) {
            return this;
        }

        @Override
        public CreateQueueResponse build() {
            return new CreateQueueResponse();
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
        }
    }
}
