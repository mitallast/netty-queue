package org.mitallast.queue.action.queue.peek;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.queue.QueueMessage;

public class PeekQueueResponse extends ActionResponse {
    private QueueMessage message;

    public PeekQueueResponse(QueueMessage message) {
        this.message = message;
    }

    public QueueMessage getMessage() {
        return message;
    }

    public void setMessage(QueueMessage message) {
        this.message = message;
    }
}