package org.mitallast.queue.crdt.log;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

public class LogEntry implements Streamable {
    private final long vclock;
    private final long id;
    private final Streamable event;

    public LogEntry(long vclock, long id, Streamable event) {
        this.vclock = vclock;
        this.id = id;
        this.event = event;
    }

    public LogEntry(StreamInput stream) {
        this.vclock = stream.readLong();
        this.id = stream.readLong();
        this.event = stream.readStreamable();
    }

    @Override
    public void writeTo(StreamOutput stream) {
        stream.writeLong(vclock);
        stream.writeLong(id);
        stream.writeClass(event.getClass());
        stream.writeStreamable(event);
    }

    public long vclock() {
        return vclock;
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
            "vclock=" + vclock +
            ", id=" + id +
            ", event=" + event +
            '}';
    }
}
