package org.mitallast.queue.raft.resource.result;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;

public class LongResult implements Streamable {
    private long value;

    public LongResult() {
    }

    public LongResult(long value) {
        this.value = value;
    }

    public long get() {
        return value;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        value = stream.readLong();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeLong(value);
    }
}
