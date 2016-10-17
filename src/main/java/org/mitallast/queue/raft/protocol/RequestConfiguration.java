package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft.RaftMessage;

import java.io.IOException;

public class RequestConfiguration implements RaftMessage {

    public static final RequestConfiguration INSTANCE = new RequestConfiguration();

    public static RequestConfiguration read(StreamInput stream) throws IOException {
        return INSTANCE;
    }

    private RequestConfiguration() {
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {

    }
}
