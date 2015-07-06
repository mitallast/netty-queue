package org.mitallast.queue.raft.log.entry;

public abstract class OperationEntry<E extends OperationEntry<E>> extends SessionEntry<E> {
    public OperationEntry(long index, long term, long timestamp, long session) {
        super(index, term, timestamp, session);
    }

    public static abstract class Builder<B extends Builder<B, E>, E extends OperationEntry> extends SessionEntry.Builder<B, E> {
    }
}
