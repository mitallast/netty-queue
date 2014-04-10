package org.mitallast.queue.queues;

import org.mitallast.queue.QueueException;

public class QueueAlreadyExistsException extends QueueException {
    public QueueAlreadyExistsException(String msg) {
        super(msg);
    }

    public QueueAlreadyExistsException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
