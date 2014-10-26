package org.mitallast.queue.action.queue.dequeue;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.queue.QueueMessage;

public class DeQueueResponse extends ActionResponse {
    private QueueMessage message;

    public DeQueueResponse(QueueMessage message) {
        this.message = message;
    }

    public QueueMessage getMessage() {
        return message;
    }

    public void setMessage(QueueMessage message) {
        this.message = message;
    }
}