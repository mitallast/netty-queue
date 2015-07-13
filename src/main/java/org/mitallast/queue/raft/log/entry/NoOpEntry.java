package org.mitallast.queue.raft.log.entry;

public class NoOpEntry extends RaftLogEntry<NoOpEntry> {
    public NoOpEntry(long index, long term) {
        super(index, term);
    }

    @Override
    public String toString() {
        return "NoOpEntry{" +
            "index=" + index +
            ", term=" + term +
            "}";
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends RaftLogEntry.Builder<Builder, NoOpEntry> {

        public NoOpEntry build() {
            return new NoOpEntry(index, term);
        }
    }
}
