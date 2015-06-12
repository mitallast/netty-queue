package org.mitallast.queue.action.queue.peek;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.transport.netty.ResponseMapper;

import java.io.IOException;

public class PeekQueueResponse extends ActionResponse {

    public final static ResponseMapper<PeekQueueResponse> mapper = new ResponseMapper<>(PeekQueueResponse::new);

    private QueueMessage message;

    public PeekQueueResponse() {
    }

    public PeekQueueResponse(QueueMessage message) {
        this.message = message;
    }

    public QueueMessage getMessage() {
        return message;
    }

    public void setMessage(QueueMessage message) {
        this.message = message;
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