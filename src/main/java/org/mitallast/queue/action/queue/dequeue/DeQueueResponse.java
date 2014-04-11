package org.mitallast.queue.action.queue.dequeue;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.queue.QueueMessage;

public class DeQueueResponse<Message> extends ActionResponse {
    private QueueMessage<Message> message;

    public DeQueueResponse(QueueMessage<Message> message) {
        this.message = message;
    }

    public QueueMessage<Message> getMessage() {
        return message;
    }

    public void setMessage(QueueMessage<Message> message) {
        this.message = message;
    }
}