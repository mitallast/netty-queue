package org.mitallast.queue.raft;

public class IllegalMemberStateException extends RaftException {
    private static final RaftError TYPE = RaftError.ILLEGAL_MEMBER_STATE_ERROR;

    public IllegalMemberStateException(String message, Object... args) {
        super(TYPE, message, args);
    }

    public IllegalMemberStateException(Throwable cause, String message, Object... args) {
        super(TYPE, cause, message, args);
    }

    public IllegalMemberStateException(Throwable cause) {
        super(TYPE, cause);
    }
}
