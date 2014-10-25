package org.mitallast.queue.queue;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.QueueException;

import java.util.UUID;

public class QueueMessageUuidDuplicateException extends QueueException {

    public QueueMessageUuidDuplicateException(UUID msg) {
        super(msg.toString());
    }

    @Override
    public HttpResponseStatus status() {
        return HttpResponseStatus.CONFLICT;
    }
}
