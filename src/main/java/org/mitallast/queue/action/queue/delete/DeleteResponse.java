package org.mitallast.queue.action.queue.delete;

import org.mitallast.queue.action.ActionResponse;

public class DeleteResponse extends ActionResponse {
    private boolean acknowledged;

    public DeleteResponse(boolean acknowledged) {
        this.acknowledged = acknowledged;
    }

    public void setAcknowledged(boolean acknowledged) {
        this.acknowledged = acknowledged;
    }

    public boolean isAcknowledged() {
        return acknowledged;
    }
}
