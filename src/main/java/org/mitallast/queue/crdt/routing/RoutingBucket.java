package org.mitallast.queue.crdt.routing;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.mitallast.queue.common.Immutable;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class RoutingBucket implements Streamable {
    private final int index;
    private final ImmutableSet<DiscoveryNode> members;
    private final ImmutableMap<Long, Resource> resources;

    public RoutingBucket(int index) {
        this(index, ImmutableSet.of(), ImmutableMap.of());
    }

    public RoutingBucket(int index, ImmutableSet<DiscoveryNode> members, ImmutableMap<Long, Resource> resources) {
        this.index = index;
        this.members = members;
        this.resources = resources;
    }

    public RoutingBucket(StreamInput stream) throws IOException {
        index = stream.readInt();
        members = stream.readStreamableSet(DiscoveryNode::new);
        resources = Immutable.group(stream.readStreamableList(Resource::new), Resource::id);
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeInt(index);
        stream.writeStreamableSet(members);
        stream.writeStreamableList(resources.values());
    }

    public int index() {
        return index;
    }

    public ImmutableSet<DiscoveryNode> members() {
        return members;
    }

    public ImmutableMap<Long, Resource> resources() {
        return resources;
    }

    public RoutingBucket withMember(DiscoveryNode node) {
        return new RoutingBucket(
            index,
            Immutable.compose(members, node),
            resources
        );
    }

    public RoutingBucket withResource(Resource resource) {
        return new RoutingBucket(
            index,
            members,
            Immutable.compose(resources, resource.id(), resource)
        );
    }

    public RoutingBucket withoutResource(long resource) {
        return new RoutingBucket(
            index,
            members,
            Immutable.subtract(resources, resource)
        );
    }

    public boolean hasResource(long id) {
        return resources.containsKey(id);
    }

    public Resource resource(long id) {
        return resources.get(id);
    }

    public RoutingBucket filterMembers(ImmutableSet<DiscoveryNode> members) {
        return new RoutingBucket(
            index,
            Immutable.filter(this.members, members::contains),
            resources
        );
    }
}
