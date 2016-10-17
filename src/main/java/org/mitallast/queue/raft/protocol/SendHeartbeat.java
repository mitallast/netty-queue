package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft.RaftMessage;

import java.io.IOException;

public class SendHeartbeat implements RaftMessage {

    public final static SendHeartbeat INSTANCE = new SendHeartbeat();

    public static SendHeartbeat read(StreamInput stream) throws IOException {
        return INSTANCE;
    }

    private SendHeartbeat() {
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
    }
}
