package org.mitallast.queue.crdt.bucket

interface BucketFactory {
    fun create(index: Int, replica: Long): Bucket
}
