package org.mitallast.queue.action.queue.enqueue;

import org.mitallast.queue.action.ActionResponse;

public class EnQueueResponse extends ActionResponse {

    private long index;

    public EnQueueResponse(long index) {
        this.index = index;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }
}
