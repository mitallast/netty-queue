package org.mitallast.queue.action.queues.remove;

import org.mitallast.queue.action.support.AcknowledgedResponse;

public class RemoveQueueResponse extends AcknowledgedResponse {

    public RemoveQueueResponse() {
    }

    public RemoveQueueResponse(boolean acknowledged) {
        super(acknowledged);
    }
}
