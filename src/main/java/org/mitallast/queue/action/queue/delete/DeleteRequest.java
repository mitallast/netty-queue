package org.mitallast.queue.action.queue.delete;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.action.ActionType;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.strings.Strings;

import java.io.IOException;
import java.util.UUID;

import static org.mitallast.queue.action.ValidateActions.addValidationError;

public class DeleteRequest extends ActionRequest {

    private String queue;
    private UUID messageUUID;

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public UUID getMessageUUID() {
        return messageUUID;
    }

    public void setMessageUUID(UUID messageUUID) {
        this.messageUUID = messageUUID;
    }

    @Override
    public ActionType actionType() {
        return ActionType.QUEUE_DELETE;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isEmpty(queue)) {
            validationException = addValidationError("queue is missing", null);
        }
        if (messageUUID == null) {
            validationException = addValidationError("messageUUID is missing", null);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        queue = stream.readTextOrNull();
        messageUUID = stream.readUUIDOrNull();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeTextOrNull(queue);
        stream.writeUUIDOrNull(messageUUID);
    }
}