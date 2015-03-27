package org.mitallast.queue.action.queue.stats;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.common.strings.Strings;

import static org.mitallast.queue.action.ValidateActions.addValidationError;

public class QueueStatsRequest extends ActionRequest {
    private String queue;

    public QueueStatsRequest() {
    }

    public QueueStatsRequest(String queue) {
        this.queue = queue;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isEmpty(queue)) {
            validationException = addValidationError("queue is missing", null);
        }
        return validationException;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }
}
