package org.mitallast.queue.crdt.event;

public class ClosedLogSynced {
    private final int bucket;
    private final long replica;

    public ClosedLogSynced(int bucket, long replica) {
        this.bucket = bucket;
        this.replica = replica;
    }

    public int bucket() {
        return bucket;
    }

    public long replica() {
        return replica;
    }
}
