package org.mitallast.queue.action.queue.delete;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.queue.QueueMessage;

import java.io.IOException;

public class DeleteResponse extends ActionResponse {
    private QueueMessage message;

    public DeleteResponse() {
    }

    public DeleteResponse(QueueMessage message) {
        this.message = message;
    }

    public QueueMessage getMessage() {
        return message;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        if (stream.readBoolean()) {
            message = new QueueMessage();
            message.readFrom(stream);
        } else {
            message = null;
        }
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        if (message != null) {
            stream.writeBoolean(true);
            message.writeTo(stream);
        } else {
            stream.writeBoolean(false);
        }
    }
}
