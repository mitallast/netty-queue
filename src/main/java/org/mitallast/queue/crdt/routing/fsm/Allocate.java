package org.mitallast.queue.crdt.routing.fsm;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class Allocate implements Streamable {
    private final long resource;
    private final DiscoveryNode node;

    public Allocate(long resource, DiscoveryNode node) {
        this.resource = resource;
        this.node = node;
    }

    public Allocate(StreamInput stream) throws IOException {
        resource = stream.readLong();
        node = stream.readStreamable(DiscoveryNode::new);
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeLong(resource);
        stream.writeStreamable(node);
    }

    public long resource() {
        return resource;
    }

    public DiscoveryNode node() {
        return node;
    }
}
