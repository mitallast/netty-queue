package org.mitallast.queue.raft.log.entry;

import org.mitallast.queue.common.builder.Entry;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;

import java.io.IOException;


public abstract class LogEntry<E extends LogEntry<E>> implements Entry<E> {
    protected final long index;
    protected final long term;

    public LogEntry(long index, long term) {
        this.index = index;
        this.term = term;
    }

    public long index() {
        return index;
    }

    public long term() {
        return term;
    }

    public static abstract class Builder<B extends Builder<B, E>, E extends LogEntry> implements EntryBuilder<E> {
        protected long index;
        protected long term;

        @SuppressWarnings("unchecked")
        public B from(E entry) {
            this.index = entry.index;
            this.term = entry.term;
            return (B) this;
        }

        @SuppressWarnings("unchecked")
        public final B setIndex(long index) {
            this.index = index;
            return (B) this;
        }

        @SuppressWarnings("unchecked")
        public final B setTerm(long term) {
            this.term = term;
            return (B) this;
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            index = stream.readLong();
            term = stream.readLong();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeLong(index);
            stream.writeLong(term);
        }
    }
}
