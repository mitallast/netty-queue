package org.mitallast.queue.raft;

public class WriteException extends RaftException {
    private static final RaftError TYPE = RaftError.COMMAND_ERROR;

    public WriteException(String message, Object... args) {
        super(TYPE, message, args);
    }

    public WriteException(Throwable cause, String message, Object... args) {
        super(TYPE, cause, message, args);
    }

    public WriteException(Throwable cause) {
        super(TYPE, cause);
    }

}
