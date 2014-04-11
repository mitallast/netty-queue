package org.mitallast.queue.client;

import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.queue.dequeue.DeQueueAction;
import org.mitallast.queue.action.queue.dequeue.DeQueueRequest;
import org.mitallast.queue.action.queue.dequeue.DeQueueResponse;
import org.mitallast.queue.action.queue.enqueue.EnQueueAction;
import org.mitallast.queue.action.queue.enqueue.EnQueueRequest;
import org.mitallast.queue.action.queue.enqueue.EnQueueResponse;

public class QueueClient {
    private final EnQueueAction enQueueAction;
    private final DeQueueAction deQueueAction;

    public QueueClient(EnQueueAction enQueueAction, DeQueueAction deQueueAction) {
        this.enQueueAction = enQueueAction;
        this.deQueueAction = deQueueAction;
    }

    public void enQueueRequest(EnQueueRequest request, ActionListener<EnQueueResponse> listener) {
        enQueueAction.execute(request, listener);
    }

    public void deQueueRequest(DeQueueRequest request, ActionListener<DeQueueResponse> listener) {
        deQueueAction.execute(request, listener);
    }
}
