package org.mitallast.queue.action.queue.delete;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.queue.QueueMessage;

public class DeleteResponse extends ActionResponse {
    private final QueueMessage message;

    public DeleteResponse(QueueMessage message) {
        this.message = message;
    }

    public QueueMessage getMessage() {
        return message;
    }
}
