package org.mitallast.queue.raft.resource.result;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;

public class BooleanResult implements Streamable {
    private boolean value;

    public BooleanResult() {
    }

    public BooleanResult(boolean value) {
        this.value = value;
    }

    public boolean get() {
        return value;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        value = stream.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeBoolean(value);
    }
}
