package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft.RaftMessage;

import java.io.IOException;

public class WhoIsTheLeader implements RaftMessage {

    public final static WhoIsTheLeader INSTANCE = new WhoIsTheLeader();

    public static WhoIsTheLeader read(StreamInput stream) throws IOException {
        return INSTANCE;
    }

    private WhoIsTheLeader() {
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {

    }
}
