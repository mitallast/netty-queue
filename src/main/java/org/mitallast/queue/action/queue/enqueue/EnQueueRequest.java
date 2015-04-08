package org.mitallast.queue.action.queue.enqueue;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.common.strings.Strings;
import org.mitallast.queue.queue.QueueMessage;

import java.io.IOException;

import static org.mitallast.queue.action.ValidateActions.addValidationError;

public class EnQueueRequest extends ActionRequest implements Streamable {

    private String queue;

    private QueueMessage message;

    public EnQueueRequest() {
    }

    public EnQueueRequest(String queue, QueueMessage message) {
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
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isEmpty(queue)) {
            validationException = addValidationError("queue is missing", null);
        }
        if (message == null) {
            validationException = addValidationError("message is missing", validationException);
        } else if (message.getMessageType() == null) {
            validationException = addValidationError("message type is missing", validationException);
        } else if (message.getSource() == null) {
            validationException = addValidationError("message source is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        queue = stream.readTextOrNull();
        if (stream.readBoolean()) {
            message = new QueueMessage();
            message.readFrom(stream);
        }
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeTextOrNull(queue);
        if (message != null) {
            stream.writeBoolean(true);
            message.writeTo(stream);
        } else {
            stream.writeBoolean(false);
        }
    }
}
