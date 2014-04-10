package org.mitallast.queue.action.support;

import org.mitallast.queue.action.ActionResponse;

public abstract class AcknowledgedResponse extends ActionResponse {
    private boolean acknowledged;

    protected AcknowledgedResponse() {

    }

    protected AcknowledgedResponse(boolean acknowledged) {
        this.acknowledged = acknowledged;
    }

    public final boolean isAcknowledged() {
        return acknowledged;
    }
}
