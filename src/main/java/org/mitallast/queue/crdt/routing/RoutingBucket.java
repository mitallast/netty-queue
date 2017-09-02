package org.mitallast.queue.crdt.routing;

import javaslang.collection.HashMap;
import javaslang.collection.Map;
import javaslang.collection.Seq;
import javaslang.collection.Set;
import javaslang.control.Option;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.transport.DiscoveryNode;

public class RoutingBucket implements Message {
    public static final Codec<RoutingBucket> codec = Codec.Companion.of(
        RoutingBucket::new,
        RoutingBucket::index,
        RoutingBucket::replicaSeq,
        RoutingBucket::resourceSeq,
        Codec.Companion.intCodec(),
        Codec.Companion.seqCodec(RoutingReplica.codec),
        Codec.Companion.seqCodec(Resource.codec)
    );

    private final int index;
    private final Map<Long, RoutingReplica> replicas;
    private final Map<Long, Resource> resources;

    public RoutingBucket(int index) {
        this(index, HashMap.empty(), HashMap.empty());
    }

    public RoutingBucket(int index, Seq<RoutingReplica> replicas, Seq<Resource> resources) {
        this.index = index;
        this.replicas = replicas.toMap(RoutingReplica::id, r -> r);
        this.resources = resources.toMap(Resource::id, r -> r);
    }

    public RoutingBucket(int index, Map<Long, RoutingReplica> replicas, Map<Long, Resource> resources) {
        this.index = index;
        this.replicas = replicas;
        this.resources = resources;
    }

    public int index() {
        return index;
    }

    public Seq<RoutingReplica> replicaSeq() {
        return replicas.values();
    }

    public Map<Long, RoutingReplica> replicas() {
        return replicas;
    }

    public Option<RoutingReplica> replica(DiscoveryNode member) {
        return replicas.values().find(replica -> replica.member().equals(member));
    }

    public boolean exists(DiscoveryNode member) {
        return replicas.values().exists(replica -> replica.member().equals(member));
    }

    public Seq<Resource> resourceSeq() {
        return resources.values();
    }

    public Map<Long, Resource> resources() {
        return resources;
    }

    public RoutingBucket withResource(Resource resource) {
        return new RoutingBucket(
            index,
            replicas,
            resources.put(resource.id(), resource)
        );
    }

    public RoutingBucket withoutResource(long resource) {
        return new RoutingBucket(
            index,
            replicas,
            resources.remove(resource)
        );
    }

    public boolean hasResource(long id) {
        return resources.containsKey(id);
    }

    public Resource resource(long id) {
        return resources.getOrElse(id, null);
    }

    public RoutingBucket withReplica(RoutingReplica member) {
        return new RoutingBucket(
            index,
            replicas.put(member.id(), member),
            resources
        );
    }

    public RoutingBucket filterReplicas(Set<DiscoveryNode> members) {
        return new RoutingBucket(
            index,
            replicas.filterValues(replica -> members.contains(replica.member())),
            resources
        );
    }

    public RoutingBucket withoutReplica(long replica) {
        return new RoutingBucket(
            index,
            replicas.remove(replica),
            resources
        );
    }

    @Override
    public String toString() {
        return "RoutingBucket{" +
            "index=" + index +
            ", members=" + replicas +
            ", resources=" + resources +
            '}';
    }
}
