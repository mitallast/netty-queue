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
        message = stream.readStreamableOrNull(QueueMessage::new);
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeStreamableOrNull(message);
    }
}
