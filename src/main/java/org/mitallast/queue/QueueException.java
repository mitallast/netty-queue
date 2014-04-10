package org.mitallast.queue;

import io.netty.handler.codec.http.HttpResponseStatus;

public class QueueException extends RuntimeException {

    public QueueException() {
        super();
    }

    public QueueException(Throwable ex) {
        super(ex);
    }

    public QueueException(String msg) {
        super(msg);
    }

    public QueueException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public HttpResponseStatus status() {
        Throwable cause = unwrapCause();
        if (cause == this) {
            return HttpResponseStatus.INTERNAL_SERVER_ERROR;
        } else if (cause instanceof QueueException) {
            return ((QueueException) cause).status();
        } else if (cause instanceof IllegalArgumentException) {
            return HttpResponseStatus.BAD_REQUEST;
        } else {
            return HttpResponseStatus.INTERNAL_SERVER_ERROR;
        }
    }

    public Throwable unwrapCause() {
        Throwable result = this;
        while (result instanceof QueueException) {
            if (result.getCause() == null) {
                return result;
            }
            if (result.getCause() == result) {
                return result;
            }
            result = result.getCause();
        }
        return result;
    }

    public Throwable getRootCause() {
        Throwable rootCause = this;
        Throwable cause = getCause();
        while (cause != null && cause != rootCause) {
            rootCause = cause;
            cause = cause.getCause();
        }
        return rootCause;
    }
}
