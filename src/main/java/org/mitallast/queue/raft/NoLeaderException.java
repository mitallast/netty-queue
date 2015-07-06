package org.mitallast.queue.raft;

public class NoLeaderException extends RaftException {
    private static final RaftError TYPE = RaftError.NO_LEADER_ERROR;

    public NoLeaderException(String message, Object... args) {
        super(TYPE, message, args);
    }

    public NoLeaderException(Throwable cause, String message, Object... args) {
        super(TYPE, cause, message, args);
    }

    public NoLeaderException(Throwable cause) {
        super(TYPE, cause);
    }

}
