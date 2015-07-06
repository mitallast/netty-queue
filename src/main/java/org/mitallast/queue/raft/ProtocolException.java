package org.mitallast.queue.raft;

public class ProtocolException extends RaftException {
    private static final RaftError TYPE = RaftError.APPLICATION_ERROR;

    public ProtocolException(String message, Object... args) {
        super(TYPE, message, args);
    }

    public ProtocolException(Throwable cause, String message, Object... args) {
        super(TYPE, cause, message, args);
    }

    public ProtocolException(Throwable cause) {
        super(TYPE, cause);
    }

}
