package org.mitallast.queue.raft.log.entry;

public class RegisterEntry extends TimestampedEntry<RegisterEntry> {
    public RegisterEntry(long index, long term, long timestamp) {
        super(index, term, timestamp);
    }

    @Override
    public String toString() {
        return "RegisterEntry{" +
            "index=" + index +
            ", term=" + term +
            ", timestamp=" + timestamp +
            "}";
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends TimestampedEntry.Builder<Builder, RegisterEntry> {
        public RegisterEntry build() {
            return new RegisterEntry(index, term, timestamp);
        }
    }
}
