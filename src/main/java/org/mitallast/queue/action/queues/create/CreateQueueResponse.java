package org.mitallast.queue.action.queues.create;

import org.mitallast.queue.action.support.AcknowledgedResponse;

public class CreateQueueResponse extends AcknowledgedResponse {

    public CreateQueueResponse() {
    }

    public CreateQueueResponse(boolean acknowledged) {
        super(acknowledged);
    }
}
