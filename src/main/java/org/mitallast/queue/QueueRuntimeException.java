package org.mitallast.queue;

public class QueueRuntimeException extends QueueException {

    public QueueRuntimeException(Throwable cause) {
        super(cause);
    }

    public QueueRuntimeException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
