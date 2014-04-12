package org.mitallast.queue.queues;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.QueueException;

public class QueueMissingException extends QueueException {

    public QueueMissingException(String msg) {
        super(msg);
    }

    public QueueMissingException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public HttpResponseStatus status() {
        return HttpResponseStatus.NOT_FOUND;
    }
}
