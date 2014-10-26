package org.mitallast.queue.action.queue.get;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.queue.QueueMessage;

public class GetResponse extends ActionResponse {
    private QueueMessage message;

    public GetResponse(QueueMessage message) {
        this.message = message;
    }

    public QueueMessage getMessage() {
        return message;
    }

    public void setMessage(QueueMessage message) {
        this.message = message;
    }
}