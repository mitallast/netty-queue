package org.mitallast.queue.raft;

public class ReadException extends RaftException {
    private static final RaftError TYPE = RaftError.QUERY_ERROR;

    public ReadException(String message, Object... args) {
        super(TYPE, message, args);
    }

    public ReadException(Throwable cause, String message, Object... args) {
        super(TYPE, cause, message, args);
    }

    public ReadException(Throwable cause) {
        super(TYPE, cause);
    }

}
