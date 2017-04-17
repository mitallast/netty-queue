package org.mitallast.queue.crdt.routing;

import javaslang.collection.HashMap;
import javaslang.collection.Map;
import javaslang.collection.Set;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

public class RoutingBucket implements Streamable {
    private final int index;
    private final Map<DiscoveryNode, BucketMember> members;
    private final Map<Long, Resource> resources;

    public RoutingBucket(int index) {
        this(index, HashMap.empty(), HashMap.empty());
    }

    public RoutingBucket(int index, Map<DiscoveryNode, BucketMember> members, Map<Long, Resource> resources) {
        this.index = index;
        this.members = members;
        this.resources = resources;
    }

    public RoutingBucket(StreamInput stream) {
        index = stream.readInt();
        members = stream.readSeq(BucketMember::new).toMap(BucketMember::member, r -> r);
        resources = stream.readSeq(Resource::new).toMap(Resource::id, r -> r);
    }

    @Override
    public void writeTo(StreamOutput stream) {
        stream.writeInt(index);
        stream.writeSeq(members.values());
        stream.writeSeq(resources.values());
    }

    public int index() {
        return index;
    }

    public Map<DiscoveryNode, BucketMember> members() {
        return members;
    }

    public Map<Long, Resource> resources() {
        return resources;
    }

    public RoutingBucket withMember(DiscoveryNode member) {
        return withMember(new BucketMember(member));
    }

    public RoutingBucket withMember(BucketMember member) {
        return new RoutingBucket(
            index,
            members.put(member.member(), member),
            resources
        );
    }

    public RoutingBucket withResource(Resource resource) {
        return new RoutingBucket(
            index,
            members,
            resources.put(resource.id(), resource)
        );
    }

    public RoutingBucket withoutResource(long resource) {
        return new RoutingBucket(
            index,
            members,
            resources.remove(resource)
        );
    }

    public boolean hasResource(long id) {
        return resources.containsKey(id);
    }

    public Resource resource(long id) {
        return resources.getOrElse(id, null);
    }

    public RoutingBucket filterMembers(Set<DiscoveryNode> members) {
        return new RoutingBucket(
            index,
            this.members.filterKeys(members::contains),
            resources
        );
    }

    public RoutingBucket withoutMember(DiscoveryNode member) {
        return new RoutingBucket(
            index,
            this.members.remove(member),
            resources
        );
    }

    @Override
    public String toString() {
        return "RoutingBucket{" +
            "index=" + index +
            ", members=" + members +
            ", resources=" + resources +
            '}';
    }
}
