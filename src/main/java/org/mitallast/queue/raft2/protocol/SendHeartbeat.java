package org.mitallast.queue.raft2.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft2.RaftMessage;

import java.io.IOException;

public class SendHeartbeat implements RaftMessage {

    public final static SendHeartbeat INSTANCE = new SendHeartbeat();

    protected SendHeartbeat() {
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
    }
}
