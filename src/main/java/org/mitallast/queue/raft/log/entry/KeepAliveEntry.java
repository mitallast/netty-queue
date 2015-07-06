package org.mitallast.queue.raft.log.entry;

public class KeepAliveEntry extends SessionEntry<KeepAliveEntry> {
    public KeepAliveEntry(long index, long term, long timestamp, long session) {
        super(index, term, timestamp, session);
    }

    @Override
    public String toString() {
        return "KeepAliveEntry{" +
            "index=" + index +
            ", term=" + term +
            ", timestamp=" + timestamp +
            ", session=" + session +
            "}";
    }


    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends SessionEntry.Builder<Builder, KeepAliveEntry> {

        public KeepAliveEntry build() {
            return new KeepAliveEntry(index, term, timestamp, session);
        }
    }
}
