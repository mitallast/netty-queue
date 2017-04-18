package org.mitallast.queue.crdt.routing;

import javaslang.collection.HashMap;
import javaslang.collection.Map;
import javaslang.collection.Set;
import javaslang.control.Option;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

public class RoutingBucket implements Streamable {
    private final int index;
    private final Map<Long, RoutingReplica> replicas;
    private final Map<Long, Resource> resources;

    public RoutingBucket(int index) {
        this(index, HashMap.empty(), HashMap.empty());
    }

    public RoutingBucket(int index, Map<Long, RoutingReplica> replicas, Map<Long, Resource> resources) {
        this.index = index;
        this.replicas = replicas;
        this.resources = resources;
    }

    public RoutingBucket(StreamInput stream) {
        index = stream.readInt();
        replicas = stream.readSeq(RoutingReplica::new).toMap(RoutingReplica::id, r -> r);
        resources = stream.readSeq(Resource::new).toMap(Resource::id, r -> r);
    }

    @Override
    public void writeTo(StreamOutput stream) {
        stream.writeInt(index);
        stream.writeSeq(replicas.values());
        stream.writeSeq(resources.values());
    }

    public int index() {
        return index;
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
