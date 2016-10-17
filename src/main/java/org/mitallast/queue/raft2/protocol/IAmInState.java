package org.mitallast.queue.raft2.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft2.RaftMessage;
import org.mitallast.queue.raft2.RaftState;

import java.io.IOException;

public class IAmInState implements RaftMessage {
    private RaftState state;

    protected IAmInState() {
    }

    public IAmInState(RaftState state) {
        this.state = state;
    }

    public RaftState getState() {
        return state;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        state = stream.readEnum(RaftState.class);
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeEnum(state);
    }
}
