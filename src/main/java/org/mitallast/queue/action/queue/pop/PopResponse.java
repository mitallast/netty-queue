package org.mitallast.queue.action.queue.pop;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.transport.netty.ResponseMapper;

import java.io.IOException;

public class PopResponse extends ActionResponse {

    public final static ResponseMapper<PopResponse> mapper = new ResponseMapper<>(PopResponse::new);

    private QueueMessage message;

    public PopResponse() {
    }

    public PopResponse(QueueMessage message) {
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