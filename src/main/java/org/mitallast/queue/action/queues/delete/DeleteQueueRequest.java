package org.mitallast.queue.action.queues.delete;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.action.ActionType;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;

import java.io.IOException;

import static org.mitallast.queue.action.ValidateActions.addValidationError;

public class DeleteQueueRequest extends ActionRequest {
    private String queue;
    private String reason;

    public DeleteQueueRequest() {
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
    public ActionType actionType() {
        return ActionType.QUEUES_DELETE;
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

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        queue = stream.readTextOrNull();
        reason = stream.readTextOrNull();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeTextOrNull(queue);
        stream.writeTextOrNull(reason);
    }
}
