package org.mitallast.queue.raft;

public class Commit<T extends Operation> {
    private final long index;
    private final long timestamp;
    private final Session session;
    private final T operation;

    public Commit(long index, Session session, long timestamp, T operation) {
        this.index = index;
        this.session = session;
        this.timestamp = timestamp;
        this.operation = operation;
    }

    public long index() {
        return index;
    }

    public Session session() {
        return session;
    }

    public long timestamp() {
        return timestamp;
    }

    @SuppressWarnings("unchecked")
    public Class<T> type() {
        return (Class<T>) operation.getClass();
    }

    public T operation() {
        return operation;
    }
}
