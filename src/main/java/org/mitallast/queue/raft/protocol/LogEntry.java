package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;

public class LogEntry implements Message {
    public static final Codec<LogEntry> codec = Codec.Companion.of(
        LogEntry::new,
        LogEntry::term,
        LogEntry::index,
        LogEntry::session,
        LogEntry::command,
        Codec.Companion.longCodec(),
        Codec.Companion.longCodec(),
        Codec.Companion.longCodec(),
        Codec.Companion.anyCodec()
    );

    private final long term;
    private final long index;
    private final long session;
    private final Message command;

    public LogEntry(long term, long index, long session, Message command) {
        this.term = term;
        this.index = index;
        this.session = session;
        this.command = command;
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

    public Message command() {
        return command;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LogEntry logEntry = (LogEntry) o;

        if (term != logEntry.term) return false;
        if (index != logEntry.index) return false;
        if (session != logEntry.session) return false;
        return command.equals(logEntry.command);
    }

    @Override
    public int hashCode() {
        int result = (int) (term ^ (term >>> 32));
        result = 31 * result + (int) (index ^ (index >>> 32));
        result = 31 * result + (int) (session ^ (session >>> 32));
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
