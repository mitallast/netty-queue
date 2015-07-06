package org.mitallast.queue.raft;

public class UnknownSessionException extends RaftException {
    private static final RaftError TYPE = RaftError.UNKNOWN_SESSION_ERROR;

    public UnknownSessionException(String message, Object... args) {
        super(TYPE, message, args);
    }

    public UnknownSessionException(Throwable cause, String message, Object... args) {
        super(TYPE, cause, message, args);
    }

    public UnknownSessionException(Throwable cause) {
        super(TYPE, cause);
    }

}
