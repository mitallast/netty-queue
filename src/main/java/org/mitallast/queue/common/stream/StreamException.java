package org.mitallast.queue.common.stream;

public class StreamException extends RuntimeException {
    public StreamException(String message) {
        super(message);
    }

    public StreamException(Throwable cause) {
        super(cause);
    }

    public StreamException(String message, Throwable cause) {
        super(message, cause);
    }
}
