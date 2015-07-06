package org.mitallast.queue.raft.log.entry;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;

import java.io.IOException;

public abstract class SessionEntry<E extends SessionEntry<E>> extends TimestampedEntry<E> {
    protected final long session;

    public SessionEntry(long index, long term, long timestamp, long session) {
        super(index, term, timestamp);
        this.session = session;
    }

    public long session() {
        return session;
    }

    public static abstract class Builder<B extends Builder<B, E>, E extends SessionEntry> extends TimestampedEntry.Builder<B, E> {
        protected long session;

        public B from(E entry) {
            session = entry.session;
            return super.from(entry);
        }

        @SuppressWarnings("unchecked")
        public final B setSession(long session) {
            this.session = session;
            return (B) this;
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            super.readFrom(stream);
            session = stream.readLong();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            super.writeTo(stream);
            stream.writeLong(session);
        }
    }
}
