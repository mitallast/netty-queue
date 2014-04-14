package org.mitallast.queue.queue;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.QueueException;

public class QueueMessageUidDuplicateException extends QueueException {

    public QueueMessageUidDuplicateException(String msg) {
        super(msg);
    }

    @Override
    public HttpResponseStatus status() {
        return HttpResponseStatus.CONFLICT;
    }
}
