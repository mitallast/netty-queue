package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft.RaftMessage;
import org.mitallast.queue.raft.RaftState;

import java.io.IOException;

public class IAmInState implements RaftMessage {
    private final RaftState state;

    public IAmInState(StreamInput stream) throws IOException {
        state = stream.readEnum(RaftState.class);
    }

    public IAmInState(RaftState state) {
        this.state = state;
    }

    public RaftState getState() {
        return state;
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeEnum(state);
    }
}
