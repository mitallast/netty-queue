package org.mitallast.queue.action.queue.peek;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.queue.QueueMessage;

public class PeekQueueResponse<Message> extends ActionResponse {
    private QueueMessage<Message> message;

    public PeekQueueResponse(QueueMessage<Message> message) {
        this.message = message;
    }

    public QueueMessage<Message> getMessage() {
        return message;
    }

    public void setMessage(QueueMessage<Message> message) {
        this.message = message;
    }
}