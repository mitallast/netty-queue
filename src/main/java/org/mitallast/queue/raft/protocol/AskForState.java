package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft.RaftMessage;

import java.io.IOException;

public class AskForState implements RaftMessage {

    public final static AskForState INSTANCE = new AskForState();

    public static AskForState read(StreamInput streamInput) throws IOException {
        return INSTANCE;
    }

    private AskForState() {
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
    }
}
