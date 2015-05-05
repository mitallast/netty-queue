package org.mitallast.queue.action.queue.stats;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionType;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.validation.ValidationBuilder;

import java.io.IOException;

public class QueueStatsRequest extends ActionRequest {
    private String queue;

    public QueueStatsRequest() {
    }

    public QueueStatsRequest(String queue) {
        this.queue = queue;
    }

    @Override
    public ActionType actionType() {
        return ActionType.QUEUE_STATS;
    }

    @Override
    public ValidationBuilder validate() {
        return ValidationBuilder.builder()
            .missing("queue", queue);
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeTextOrNull(queue);
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        queue = stream.readTextOrNull();
    }
}
