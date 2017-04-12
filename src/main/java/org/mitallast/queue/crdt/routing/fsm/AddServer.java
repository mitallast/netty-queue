package org.mitallast.queue.crdt.routing.fsm;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class AddServer implements Streamable {
    private final DiscoveryNode node;

    public AddServer(DiscoveryNode node) {
        this.node = node;
    }

    public AddServer(StreamInput stream) throws IOException {
        node = stream.readStreamable(DiscoveryNode::new);
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeStreamable(node);
    }

    public DiscoveryNode node() {
        return node;
    }

    @Override
    public String toString() {
        return "AddServer{" + node + '}';
    }
}
