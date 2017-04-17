package org.mitallast.queue.crdt.event;

public class ClosedLogSynced {
    private final int bucket;

    public ClosedLogSynced(int bucket) {
        this.bucket = bucket;
    }

    public int bucket() {
        return bucket;
    }
}
