package org.mitallast.queue.action.queues.delete;

import org.mitallast.queue.action.ActionResponse;

public class DeleteQueueResponse extends ActionResponse {
    private final boolean deleted;
    private final Throwable error;

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
}
