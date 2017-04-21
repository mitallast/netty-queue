package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

public class LogEntry implements Streamable {
    private final long term;
    private final long index;
    private final Streamable command;
    private final long session;

    public LogEntry(long term, long index, long session, Streamable command) {
        this.term = term;
        this.index = index;
        this.session = session;
        this.command = command;
    }

    public LogEntry(StreamInput stream) {
        term = stream.readLong();
        index = stream.readLong();
        session = stream.readLong();
        command = stream.readStreamable();
    }

    @Override
    public void writeTo(StreamOutput stream) {
        stream.writeLong(term);
        stream.writeLong(index);
        stream.writeLong(session);
        stream.writeClass(command.getClass());
        stream.writeStreamable(command);
    }

    public long term() {
        return term;
    }

    public long index() {
        return index;
    }

    public long session() {
        return session;
    }

    public Streamable command() {
        return command;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LogEntry entry = (LogEntry) o;

        return term == entry.term &&
            index == entry.index &&
            command.equals(entry.command);
    }

    @Override
    public int hashCode() {
        int result = (int) (term ^ (term >>> 32));
        result = 31 * result + (int) (index ^ (index >>> 32));
        result = 31 * result + command.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "LogEntry{" +
            "term=" + term +
            ", index=" + index +
            ", command=" + command +
            '}';
    }
}
