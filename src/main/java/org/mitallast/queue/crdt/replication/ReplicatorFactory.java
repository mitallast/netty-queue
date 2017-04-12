package org.mitallast.queue.crdt.replication;

import org.mitallast.queue.crdt.bucket.Bucket;

public interface ReplicatorFactory {
    Replicator create(Bucket bucket);
}
