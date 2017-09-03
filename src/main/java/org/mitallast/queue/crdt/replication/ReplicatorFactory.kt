package org.mitallast.queue.crdt.replication

import org.mitallast.queue.crdt.bucket.Bucket

interface ReplicatorFactory {
    fun create(bucket: Bucket): Replicator
}
