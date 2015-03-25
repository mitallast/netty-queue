package org.mitallast.queue.queues;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.QueueException;

import java.util.UUID;

public class QueueMessageNotFoundException extends QueueException {

    public QueueMessageNotFoundException(UUID uuid) {
        super("Queue message not found [" + uuid + "]");
    }

    public HttpResponseStatus status() {
        return HttpResponseStatus.NOT_FOUND;
    }
}
