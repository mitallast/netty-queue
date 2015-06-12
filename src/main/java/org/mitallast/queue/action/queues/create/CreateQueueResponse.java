package org.mitallast.queue.action.queues.create;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.transport.netty.ResponseMapper;

import java.io.IOException;

public class CreateQueueResponse extends ActionResponse {

    public final static ResponseMapper<CreateQueueResponse> mapper = new ResponseMapper<>(CreateQueueResponse::new);

    @Override
    public void readFrom(StreamInput stream) throws IOException {

    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {

    }
}
