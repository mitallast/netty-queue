package org.mitallast.queue.raft.cluster;

public class ClusterException extends RuntimeException {
    public ClusterException() {
    }

    public ClusterException(String message) {
        super(message);
    }

    public ClusterException(String message, Throwable cause) {
        super(message, cause);
    }
}
