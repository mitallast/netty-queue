package org.mitallast.queue.queue;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.QueueException;

import java.util.UUID;

public class QueueMessageUidDuplicateException extends QueueException {

    public QueueMessageUidDuplicateException(UUID msg) {
        super(msg.toString());
    }

    @Override
    public HttpResponseStatus status() {
        return HttpResponseStatus.CONFLICT;
    }
}
