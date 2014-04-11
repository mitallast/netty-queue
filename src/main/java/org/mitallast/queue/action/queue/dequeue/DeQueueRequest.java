package org.mitallast.queue.action.queue.dequeue;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionRequestValidationException;

import static org.mitallast.queue.action.ValidateActions.addValidationError;

public class DeQueueRequest extends ActionRequest {

    private String queue;

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (queue == null || queue.isEmpty()) {
            validationException = addValidationError("queue is missing", null);
        }
        return validationException;
    }
}