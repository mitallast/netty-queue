package org.mitallast.queue.raft.log.entry;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.log.entry.LogEntry;

import java.io.IOException;


public abstract class RaftLogEntry<E extends RaftLogEntry<E>> extends LogEntry<E> {
    protected final long term;

    public RaftLogEntry(long index, long term) {
        super(index);
        this.term = term;
    }

    public long index() {
        return index;
    }

    public long term() {
        return term;
    }

    public static abstract class Builder<B extends Builder<B, E>, E extends RaftLogEntry> extends LogEntry.Builder<B, E> {
        protected long term;

        public B from(E entry) {
            this.term = entry.term;
            return super.from(entry);
        }

        @SuppressWarnings("unchecked")
        public final B setTerm(long term) {
            this.term = term;
            return (B) this;
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            super.readFrom(stream);
            term = stream.readLong();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            super.writeTo(stream);
            stream.writeLong(term);
        }
    }
}
