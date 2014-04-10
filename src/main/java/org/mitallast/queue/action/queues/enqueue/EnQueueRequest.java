package org.mitallast.queue.action.queues.enqueue;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.queue.QueueMessage;

import static org.mitallast.queue.action.ValidateActions.addValidationError;

public class EnQueueRequest<Message> extends ActionRequest {

    private String queue;

    private QueueMessage<Message> message;

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public QueueMessage<Message> getMessage() {
        return message;
    }

    public void setMessage(QueueMessage<Message> message) {
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
        }
        if (message.getMessage() == null) {
            validationException = addValidationError("message is missing", validationException);
        }
        return validationException;
    }
}
