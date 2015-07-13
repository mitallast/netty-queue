package org.mitallast.queue.raft.log;

public class RaftLogException extends RuntimeException {

    public RaftLogException(String message) {
        super(message);
    }

    public RaftLogException(String message, Throwable cause) {
        super(message, cause);
    }

    public RaftLogException(Throwable cause) {
        super(cause);
    }
}
