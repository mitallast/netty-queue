package org.mitallast.queue.action.queue.get;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionRequestValidationException;

import java.util.UUID;

import static org.mitallast.queue.action.ValidateActions.addValidationError;

public class GetRequest extends ActionRequest {

    private String queue;
    private UUID uuid;

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (queue == null || queue.isEmpty()) {
            validationException = addValidationError("queue is missing", null);
        }
        if (uuid == null) {
            validationException = addValidationError("uuid is missing", null);
        }
        return validationException;
    }
}