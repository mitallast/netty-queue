package org.mitallast.queue.raft.log.entry;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;

import java.io.IOException;

public abstract class TimestampedEntry<E extends TimestampedEntry<E>> extends RaftLogEntry<E> {

    protected final long timestamp;

    public TimestampedEntry(long index, long term, long timestamp) {
        super(index, term);
        this.timestamp = timestamp;
    }

    public long timestamp() {
        return timestamp;
    }

    public static abstract class Builder<B extends Builder<B, E>, E extends TimestampedEntry> extends RaftLogEntry.Builder<B, E> {
        protected long timestamp;

        public B from(E entry) {
            timestamp = entry.timestamp;
            return super.from(entry);
        }

        @SuppressWarnings("unchecked")
        public final B setTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return (B) this;
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            super.readFrom(stream);
            timestamp = stream.readLong();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            super.writeTo(stream);
            stream.writeLong(timestamp);
        }
    }
}
