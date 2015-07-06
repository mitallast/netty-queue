package org.mitallast.queue.raft.resource.manager;


public class ResourceManagerException extends RuntimeException {

    public ResourceManagerException() {
    }

    public ResourceManagerException(String message, Object... args) {
        super(String.format(message, args));
    }

    public ResourceManagerException(Throwable cause, String message, Object... args) {
        super(String.format(message, args), cause);
    }

    public ResourceManagerException(Throwable cause) {
        super(cause);
    }
}
