package org.mitallast.queue.action.queue.push;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.validation.ValidationBuilder;
import org.mitallast.queue.queue.QueueMessage;

import java.io.IOException;

public class PushRequest extends ActionRequest {

    private String queue;

    private QueueMessage message;

    public PushRequest() {
    }

    public PushRequest(String queue, QueueMessage message) {
        this.queue = queue;
        this.message = message;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public QueueMessage getMessage() {
        return message;
    }

    public void setMessage(QueueMessage message) {
        this.message = message;
    }

    @Override
    public ValidationBuilder validate() {
        return ValidationBuilder.builder()
            .missing("queue", queue)
            .missing("message", message);
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        queue = stream.readTextOrNull();
        message = stream.readStreamableOrNull(QueueMessage::new);
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeTextOrNull(queue);
        stream.writeStreamableOrNull(message);
    }
}
