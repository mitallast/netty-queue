package org.mitallast.queue.crdt.routing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.mitallast.queue.common.Immutable;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class RoutingTable implements Streamable {
    private final ImmutableList<DiscoveryNode> members;
    private final ImmutableMap<Long, Resource> resources;

    public RoutingTable() {
        this(ImmutableList.of(), ImmutableMap.of());
    }

    public RoutingTable(ImmutableList<DiscoveryNode> members, ImmutableMap<Long, Resource> resources) {
        this.members = members;
        this.resources = resources;
    }

    public RoutingTable(ImmutableList<DiscoveryNode> members, ImmutableList<Resource> resources) {
        this(members, Immutable.group(resources, Resource::id));
    }

    public RoutingTable(StreamInput stream) throws IOException {
        this(
            stream.readStreamableList(DiscoveryNode::new),
            stream.readStreamableList(Resource::new)
        );
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeStreamableList(resources.values());
    }

    public ImmutableList<DiscoveryNode> members() {
        return members;
    }

    public ImmutableMap<Long, Resource> resources() {
        return resources;
    }

    public RoutingTable withResource(Resource resource) {
        return new RoutingTable(members, Immutable.compose(resources, resource.id(), resource));
    }

    public RoutingTable withoutResource(long id) {
        return new RoutingTable(members, Immutable.subtract(resources, id));
    }

    public RoutingTable withMember(DiscoveryNode node) {
        return new RoutingTable(Immutable.compose(members, node), resources);
    }

    public RoutingTable withoutMember(DiscoveryNode node) {
        return new RoutingTable(
            Immutable.subtract(members, node),
            Immutable.map(resources.values(), resource -> resource.withoutMember(node))
        );
    }
}
