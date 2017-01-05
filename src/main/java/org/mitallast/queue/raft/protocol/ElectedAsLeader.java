package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;

public class ElectedAsLeader implements Streamable {

    public static final ElectedAsLeader INSTANCE = new ElectedAsLeader();

    public static ElectedAsLeader read(StreamInput stream) throws IOException {
        return INSTANCE;
    }

    private ElectedAsLeader() {
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {

    }
}
