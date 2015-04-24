package org.mitallast.queue.queues;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.QueueException;

import java.util.UUID;

public class QueueTransactionMissingException extends QueueException {

    public QueueTransactionMissingException(String queue, UUID transactionUUID) {
        super("[" + queue + "][" + transactionUUID + "]");
    }

    public HttpResponseStatus status() {
        return HttpResponseStatus.NOT_FOUND;
    }
}
