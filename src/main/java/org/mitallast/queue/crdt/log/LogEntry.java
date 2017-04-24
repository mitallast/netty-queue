package org.mitallast.queue.crdt.log;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

public class LogEntry implements Streamable {
    private final long index;
    private final long id;
    private final Streamable event;

    public LogEntry(long index, long id, Streamable event) {
        this.index = index;
        this.id = id;
        this.event = event;
    }

    public LogEntry(StreamInput stream) {
        this.index = stream.readLong();
        this.id = stream.readLong();
        this.event = stream.readStreamable();
    }

    @Override
    public void writeTo(StreamOutput stream) {
        stream.writeLong(index);
        stream.writeLong(id);
        stream.writeClass(event.getClass());
        stream.writeStreamable(event);
    }

    public long index() {
        return index;
    }

    public long id() {
        return id;
    }

    public Streamable event() {
        return event;
    }

    @Override
    public String toString() {
        return "LogEntry{" +
            "index=" + index +
            ", id=" + id +
            ", event=" + event +
            '}';
    }
}
