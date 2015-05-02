package org.mitallast.queue.transport;

import org.mitallast.queue.QueueException;

public class TransportException extends QueueException {

    public TransportException(String msg) {
        super(msg);
    }

    public TransportException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
