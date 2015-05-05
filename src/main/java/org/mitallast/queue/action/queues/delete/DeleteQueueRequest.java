package org.mitallast.queue.action.queues.delete;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionType;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.strings.Strings;
import org.mitallast.queue.common.validation.ValidationBuilder;

import java.io.IOException;

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
    public ValidationBuilder validate() {
        ValidationBuilder builder = ValidationBuilder.builder();
        if (Strings.isEmpty(queue)) {
            builder = builder.missing("queue");
        }
        if (Strings.isEmpty(reason)) {
            builder = builder.missing("reason");
        }
        return builder;
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
