package org.mitallast.queue.raft;

public class Session {
    private final long id;
    private boolean expired;
    private boolean closed;

    public Session(long id) {
        this.id = id;
    }

    public long id() {
        return id;
    }

    public boolean isOpen() {
        return !closed;
    }

    protected void close() {
        closed = true;
    }

    public boolean isClosed() {
        return closed;
    }

    protected void expire() {
        expired = true;
        close();
    }

    public boolean isExpired() {
        return expired;
    }

}
