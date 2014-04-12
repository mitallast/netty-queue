package org.mitallast.queue;

import io.netty.handler.codec.http.HttpResponseStatus;

public class QueueParseException extends QueueException {

    public QueueParseException(String msg) {
        super(msg);
    }

    public QueueParseException(String msg, Throwable cause) {
        super(msg, cause);
    }

    @Override
    public HttpResponseStatus status() {
        return HttpResponseStatus.BAD_REQUEST;
    }
}
