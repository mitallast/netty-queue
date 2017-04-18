package org.mitallast.queue.crdt.bucket;

public interface BucketFactory {
    Bucket create(int index, long replica);
}
