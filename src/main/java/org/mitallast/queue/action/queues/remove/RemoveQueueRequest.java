package org.mitallast.queue.action.queues.remove;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionRequestValidationException;

import static org.mitallast.queue.action.ValidateActions.addValidationError;

public class RemoveQueueRequest extends ActionRequest {
    private String queue;
    private String reason;

    public RemoveQueueRequest() {
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (queue == null) {
            validationException = addValidationError("queue is missing", null);
        }
        if (reason == null) {
            validationException = addValidationError("reason is missing", validationException);
        }
        return validationException;
    }
}
