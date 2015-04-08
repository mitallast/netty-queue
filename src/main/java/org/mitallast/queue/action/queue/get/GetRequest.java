package org.mitallast.queue.action.queue.get;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.strings.Strings;

import java.io.IOException;
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
        if (Strings.isEmpty(queue)) {
            validationException = addValidationError("queue is missing", null);
        }
        if (uuid == null) {
            validationException = addValidationError("uuid is missing", null);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        queue = stream.readTextOrNull();
        uuid = stream.readUUIDOrNull();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeTextOrNull(queue);
        stream.writeUUIDOrNull(uuid);
    }
}