package org.mitallast.queue.action.queue.delete;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.common.Strings;

import java.util.UUID;

import static org.mitallast.queue.action.ValidateActions.addValidationError;

public class DeleteRequest extends ActionRequest {

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
        if (Strings.isEmpty(queue)) {
            validationException = addValidationError("queue is missing", null);
        }
        if (uuid == null) {
            validationException = addValidationError("uuid is missing", null);
        }
        return validationException;
    }
}