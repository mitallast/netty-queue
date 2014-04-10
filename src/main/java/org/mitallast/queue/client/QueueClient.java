package org.mitallast.queue.client;

import org.mitallast.queue.QueueRuntimeException;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.queues.enqueue.EnQueueAction;
import org.mitallast.queue.action.queues.enqueue.EnQueueRequest;
import org.mitallast.queue.action.queues.enqueue.EnQueueResponse;

import java.util.concurrent.ExecutionException;

public class QueueClient {
    private final EnQueueAction enQueueAction;

    public QueueClient(EnQueueAction enQueueAction) {
        this.enQueueAction = enQueueAction;
    }

    public EnQueueResponse enQueueRequest(EnQueueRequest request) {
        try {
            return enQueueAction.execute(request).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new QueueRuntimeException(e);
        }
    }

    public void enQueueRequest(EnQueueRequest request, ActionListener<EnQueueResponse> listener) {
        enQueueAction.execute(request, listener);
    }
}
