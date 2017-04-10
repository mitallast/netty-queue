package org.mitallast.queue.crdt.routing;

import com.google.common.collect.ImmutableSet;
import org.mitallast.queue.common.Immutable;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class Resource implements Streamable {
    private final ResourceType type;
    private final long id;
    private final int replicas;
    private final ImmutableSet<DiscoveryNode> nodes;

    public Resource(ResourceType type, long id, int replicas, ImmutableSet<DiscoveryNode> nodes) {
        this.type = type;
        this.id = id;
        this.replicas = replicas;
        this.nodes = nodes;
    }

    public Resource(StreamInput stream) throws IOException {
        this.type = stream.readEnum(ResourceType.class);
        this.id = stream.readLong();
        this.replicas = stream.readInt();
        this.nodes = stream.readStreamableSet(DiscoveryNode::new);
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeEnum(type);
        stream.writeLong(id);
        stream.writeInt(replicas);
        stream.writeStreamableSet(nodes);
    }

    public ResourceType type() {
        return type;
    }

    public Long id() {
        return id;
    }

    public int replicas() {
        return replicas;
    }

    public ImmutableSet<DiscoveryNode> nodes() {
        return nodes;
    }

    public Resource withMember(DiscoveryNode node) {
        return new Resource(
            type,
            id,
            replicas,
            Immutable.compose(nodes, node)
        );
    }

    public Resource withoutMember(DiscoveryNode node) {
        return new Resource(
            type,
            id,
            replicas,
            Immutable.subtract(nodes, node)
        );
    }
}
