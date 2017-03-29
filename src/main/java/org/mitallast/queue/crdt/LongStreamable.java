package org.mitallast.queue.crdt;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;

public class LongStreamable implements Streamable {
    private final long value;

    public LongStreamable(long value) {
        this.value = value;
    }

    public LongStreamable(StreamInput stream) throws IOException {
        value = stream.readLong();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeLong(value);
    }

    public long getValue() {
        return value;
    }
}
