package org.mitallast.queue.raft2.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;

import java.io.IOException;

public class ElectedAsLeader implements ElectionMessage {

    public static final ElectedAsLeader INSTANCE = new ElectedAsLeader();

    protected ElectedAsLeader() {
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {

    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {

    }
}
