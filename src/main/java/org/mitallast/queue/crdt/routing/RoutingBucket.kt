package org.mitallast.queue.crdt.routing

import javaslang.collection.HashMap
import javaslang.collection.Map
import javaslang.collection.Seq
import javaslang.collection.Set
import javaslang.control.Option
import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.transport.DiscoveryNode

class RoutingBucket : Message {
    val index: Int
    val replicas: Map<Long, RoutingReplica>
    val resources: Map<Long, Resource>

    constructor(index: Int, replicas: Seq<RoutingReplica>, resources: Seq<Resource>) {
        this.index = index
        this.replicas = replicas.toMap({ it.id }) { it }
        this.resources = resources.toMap({ it.id }) { it }
    }

    @JvmOverloads constructor(index: Int, replicas: Map<Long, RoutingReplica> = HashMap.empty(), resources: Map<Long, Resource> = HashMap.empty()) {
        this.index = index
        this.replicas = replicas
        this.resources = resources
    }

    fun replicaSeq(): Seq<RoutingReplica> {
        return replicas.values()
    }

    fun replica(member: DiscoveryNode): Option<RoutingReplica> {
        return replicas.values().find { replica -> replica.member == member }
    }

    fun exists(member: DiscoveryNode): Boolean {
        return replicas.values().exists { replica -> replica.member == member }
    }

    fun resourceSeq(): Seq<Resource> {
        return resources.values()
    }

    fun withResource(resource: Resource): RoutingBucket {
        return RoutingBucket(
            index,
            replicas,
            resources.put(resource.id, resource)
        )
    }

    fun withoutResource(resource: Long): RoutingBucket {
        return RoutingBucket(
            index,
            replicas,
            resources.remove(resource)
        )
    }

    fun hasResource(id: Long): Boolean {
        return resources.containsKey(id)
    }

    fun resource(id: Long): Resource {
        return resources.getOrElse(id, null)
    }

    fun withReplica(member: RoutingReplica): RoutingBucket {
        return RoutingBucket(
            index,
            replicas.put(member.id, member),
            resources
        )
    }

    fun filterReplicas(members: Set<DiscoveryNode>): RoutingBucket {
        return RoutingBucket(
            index,
            replicas.filterValues { replica -> members.contains(replica.member) },
            resources
        )
    }

    fun withoutReplica(replica: Long): RoutingBucket {
        return RoutingBucket(
            index,
            replicas.remove(replica),
            resources
        )
    }

    override fun toString(): String {
        return "RoutingBucket{" +
            "index=" + index +
            ", members=" + replicas +
            ", resources=" + resources +
            '}'
    }

    companion object {
        val codec = Codec.of(
            ::RoutingBucket,
            RoutingBucket::index,
            RoutingBucket::replicaSeq,
            RoutingBucket::resourceSeq,
            Codec.intCodec(),
            Codec.seqCodec(RoutingReplica.codec),
            Codec.seqCodec(Resource.codec)
        )
    }
}
