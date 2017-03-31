package org.mitallast.queue.crdt.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;

public class Append implements Streamable {
    private final long id;
    private final Streamable event;

    public Append(long id, Streamable event) {
        this.id = id;
        this.event = event;
    }

    public Append(StreamInput stream) throws IOException {
        id = stream.readLong();
        event = stream.readStreamable();
    }

    public long id() {
        return id;
    }

    public Streamable event() {
        return event;
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeLong(id);
        stream.writeClass(event.getClass());
        stream.writeStreamable(event);
    }
}
