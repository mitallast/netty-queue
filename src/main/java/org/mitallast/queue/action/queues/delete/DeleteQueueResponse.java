package org.mitallast.queue.action.queues.delete;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;

import java.io.IOException;

public class DeleteQueueResponse extends ActionResponse {

    private boolean deleted;
    private Throwable error;

    public DeleteQueueResponse() {
    }

    public DeleteQueueResponse(boolean deleted) {
        this(deleted, null);
    }

    public DeleteQueueResponse(boolean deleted, Throwable error) {
        this.deleted = deleted;
        this.error = error;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public Throwable getError() {
        return error;
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
