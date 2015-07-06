package org.mitallast.queue.raft.resource.result;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;

public class IntegerResult implements Streamable {
    private int value;

    public IntegerResult() {
    }

    public IntegerResult(int value) {
        this.value = value;
    }

    public int get() {
        return value;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        value = stream.readInt();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeInt(value);
    }
}
