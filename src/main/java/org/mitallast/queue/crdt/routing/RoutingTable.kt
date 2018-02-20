package org.mitallast.queue.crdt.routing

import io.vavr.collection.HashSet
import io.vavr.collection.Set
import io.vavr.collection.Vector
import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.transport.DiscoveryNode

data class RoutingTable(
    val replicas: Int,
    val members: Set<DiscoveryNode>,
    val buckets: Vector<RoutingBucket>,
    val nextReplica: Long) : Message {

    constructor(replicas: Int, buckets: Int) :
        this(replicas, HashSet.empty(), Vector.range(0, buckets).map { RoutingBucket(it) }, 0)

    fun bucketsCount(node: DiscoveryNode): Int {
        return buckets.count { bucket -> bucket.replicas.values().exists { replica -> replica.member == node } }
    }

    fun bucket(resourceId: Long): RoutingBucket {
        val bucket = java.lang.Long.hashCode(resourceId) % buckets.size()
        return buckets.get(bucket)
    }

    fun resource(id: Long): Resource {
        return bucket(id).resource(id)
    }

    fun hasResource(id: Long): Boolean {
        return bucket(id).hasResource(id)
    }

    fun withResource(resource: Resource): RoutingTable {
        val bucket = bucket(resource.id).withResource(resource)
        return RoutingTable(
            replicas,
            members,
            buckets.update(bucket.index, bucket),
            nextReplica
        )
    }

    fun withoutResource(id: Long): RoutingTable {
        val bucket = bucket(id).withoutResource(id)
        return RoutingTable(
            replicas,
            members,
            buckets.update(bucket.index, bucket),
            nextReplica
        )
    }

    fun withReplica(bucket: Int, member: DiscoveryNode): RoutingTable {
        val updated = buckets.get(bucket).withReplica(RoutingReplica(nextReplica, member))
        return RoutingTable(
            replicas,
            members,
            buckets.update(bucket, updated),
            nextReplica + 1
        )
    }

    fun withReplica(bucket: Int, replica: RoutingReplica): RoutingTable {
        val updated = buckets.get(bucket).withReplica(replica)
        return RoutingTable(
            replicas,
            members,
            buckets.update(bucket, updated),
            nextReplica
        )
    }

    fun withMembers(members: Set<DiscoveryNode>): RoutingTable {
        return RoutingTable(
            replicas,
            members,
            buckets.map { bucket -> bucket.filterReplicas(members) },
            nextReplica
        )
    }

    fun withoutReplica(bucket: Int, replica: Long): RoutingTable {
        val updated = buckets.get(bucket).withoutReplica(replica)
        return RoutingTable(
            replicas,
            members,
            buckets.update(bucket, updated),
            nextReplica
        )
    }

    companion object {
        val codec = Codec.of(
            ::RoutingTable,
            RoutingTable::replicas,
            RoutingTable::members,
            RoutingTable::buckets,
            RoutingTable::nextReplica,
            Codec.intCodec(),
            Codec.setCodec(DiscoveryNode.codec),
            Codec.vectorCodec(RoutingBucket.codec),
            Codec.longCodec()
        )
    }
}
