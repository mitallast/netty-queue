package org.mitallast.queue.raft;

public class ApplicationException extends RaftException {
    private static final RaftError TYPE = RaftError.APPLICATION_ERROR;

    public ApplicationException(String message, Object... args) {
        super(TYPE, message, args);
    }

    public ApplicationException(Throwable cause, String message, Object... args) {
        super(TYPE, cause, message, args);
    }

    public ApplicationException(Throwable cause) {
        super(TYPE, cause);
    }

}
