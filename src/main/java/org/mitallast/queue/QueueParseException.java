package org.mitallast.queue;

public class QueueParseException extends QueueException {
    public QueueParseException(String msg) {
        super(msg);
    }

    public QueueParseException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
