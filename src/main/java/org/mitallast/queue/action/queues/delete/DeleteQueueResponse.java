package org.mitallast.queue.action.queues.delete;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;

import java.io.IOException;

public class DeleteQueueResponse implements ActionResponse<DeleteQueueResponse.Builder, DeleteQueueResponse> {
    private final boolean deleted;
    private final Throwable error;

    private DeleteQueueResponse(boolean deleted, Throwable error) {
        this.deleted = deleted;
        this.error = error;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public Throwable error() {
        return error;
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements EntryBuilder<Builder, DeleteQueueResponse> {
        private boolean deleted;
        private Throwable error;

        @Override
        public Builder from(DeleteQueueResponse entry) {
            deleted = entry.deleted;
            error = entry.error;
            return this;
        }

        public Builder setDeleted(boolean deleted) {
            this.deleted = deleted;
            return this;
        }

        public Builder setError(Throwable error) {
            this.error = error;
            return this;
        }

        @Override
        public DeleteQueueResponse build() {
            return new DeleteQueueResponse(deleted, error);
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
