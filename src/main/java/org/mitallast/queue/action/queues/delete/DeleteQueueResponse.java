package org.mitallast.queue.action.queues.delete;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.action.ActionResponseError;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;

import java.io.IOException;

public class DeleteQueueResponse implements ActionResponse<DeleteQueueResponse> {
    private final boolean deleted;

    private DeleteQueueResponse(boolean deleted) {
        this.deleted = deleted;
    }

    public boolean isDeleted() {
        return deleted;
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<DeleteQueueResponse> {
        private boolean deleted;
        private ActionResponseError error;

        private Builder from(DeleteQueueResponse entry) {
            deleted = entry.deleted;
            return this;
        }

        public Builder setDeleted(boolean deleted) {
            this.deleted = deleted;
            return this;
        }

        public Builder setError(ActionResponseError error) {
            this.error = error;
            return this;
        }

        @Override
        public DeleteQueueResponse build() {
            return new DeleteQueueResponse(deleted);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            deleted = stream.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeBoolean(deleted);
        }
    }
}
