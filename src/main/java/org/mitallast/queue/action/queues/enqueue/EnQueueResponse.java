package org.mitallast.queue.action.queues.enqueue;

import org.mitallast.queue.action.support.AcknowledgedResponse;

public class EnQueueResponse extends AcknowledgedResponse {
    public EnQueueResponse() {
    }

    public EnQueueResponse(boolean acknowledged) {
        super(acknowledged);
    }
}
