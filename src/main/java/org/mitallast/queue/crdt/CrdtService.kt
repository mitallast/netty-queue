package org.mitallast.queue.crdt

import io.vavr.concurrent.Future
import org.mitallast.queue.crdt.bucket.Bucket
import org.mitallast.queue.crdt.routing.ResourceType
import org.mitallast.queue.crdt.routing.RoutingTable

interface CrdtService {

    fun addResource(id: Long, resourceType: ResourceType): Future<Boolean>

    fun routingTable(): RoutingTable

    fun bucket(index: Int): Bucket?

    fun bucket(resourceId: Long): Bucket?
}
