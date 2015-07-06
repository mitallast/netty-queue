package org.mitallast.queue.raft;

public class InternalException extends RaftException {
    private static final RaftError TYPE = RaftError.ILLEGAL_MEMBER_STATE_ERROR;

    public InternalException(String message, Object... args) {
        super(TYPE, message, args);
    }

    public InternalException(Throwable cause, String message, Object... args) {
        super(TYPE, cause, message, args);
    }

    public InternalException(Throwable cause) {
        super(TYPE, cause);
    }

}
