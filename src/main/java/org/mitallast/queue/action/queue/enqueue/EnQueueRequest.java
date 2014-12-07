package org.mitallast.queue.action.queue.enqueue;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.queue.QueueMessage;

import static org.mitallast.queue.action.ValidateActions.addValidationError;

public class EnQueueRequest extends ActionRequest {

    private String queue;

    private QueueMessage message;

    public EnQueueRequest() {
    }

    public EnQueueRequest(String queue, QueueMessage message) {
        this.queue = queue;
        this.message = message;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public QueueMessage getMessage() {
        return message;
    }

    public void setMessage(QueueMessage message) {
        this.message = message;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (queue == null || queue.isEmpty()) {
            validationException = addValidationError("queue is missing", null);
        }
        if (message == null) {
            validationException = addValidationError("message is missing", validationException);
        } else if (message.getSource() == null) {
            validationException = addValidationError("message is missing", validationException);
        }
        return validationException;
    }
}
