package org.mitallast.queue.queues;

import org.mitallast.queue.QueueException;

public class QueueMissingException extends QueueException {
    public QueueMissingException(String msg) {
        super(msg);
    }

    public QueueMissingException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
