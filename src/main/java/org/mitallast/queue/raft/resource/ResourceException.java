package org.mitallast.queue.raft.resource;

public class ResourceException extends RuntimeException {

    public ResourceException() {
    }

    public ResourceException(String message, Object... args) {
        super(String.format(message, args));
    }

    public ResourceException(Throwable cause, String message, Object... args) {
        super(String.format(message, args), cause);
    }

    public ResourceException(Throwable cause) {
        super(cause);
    }
}
