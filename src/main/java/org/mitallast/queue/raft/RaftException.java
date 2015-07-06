package org.mitallast.queue.raft;

public class RaftException extends RuntimeException {
    private final RaftError type;

    public RaftException(RaftError type, String message, Object... args) {
        super(String.format(message, args));
        if (type == null)
            throw new NullPointerException("type cannot be null");
        this.type = type;
    }

    public RaftException(RaftError type, Throwable cause, String message, Object... args) {
        super(String.format(message, args), cause);
        if (type == null)
            throw new NullPointerException("type cannot be null");
        this.type = type;
    }

    public RaftException(RaftError type, Throwable cause) {
        super(cause);
        if (type == null)
            throw new NullPointerException("type cannot be null");
        this.type = type;
    }

    public RaftException(RaftError type) {
        super(type.name());
        this.type = type;
    }

    public RaftError getType() {
        return type;
    }

}
