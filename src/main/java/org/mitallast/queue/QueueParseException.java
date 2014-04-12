package org.mitallast.queue;

import io.netty.handler.codec.http.HttpResponseStatus;

public class QueueParseException extends QueueException {

    @Override
    public HttpResponseStatus status() {
        return HttpResponseStatus.BAD_REQUEST;
    }

    public QueueParseException(String msg) {
        super(msg);
    }

    @Override
    public HttpResponseStatus status() {
        return HttpResponseStatus.BAD_REQUEST;
    }

    public QueueParseException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
