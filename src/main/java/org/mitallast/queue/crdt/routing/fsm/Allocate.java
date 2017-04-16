package org.mitallast.queue.crdt.routing.fsm;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

public class Allocate implements Streamable {
    private final int bucket;
    private final DiscoveryNode node;

    public Allocate(int bucket, DiscoveryNode node) {
        this.bucket = bucket;
        this.node = node;
    }

    public Allocate(StreamInput stream) {
        bucket = stream.readInt();
        node = stream.readStreamable(DiscoveryNode::new);
    }

    @Override
    public void writeTo(StreamOutput stream) {
        stream.writeInt(bucket);
        stream.writeStreamable(node);
    }

    public int bucket() {
        return bucket;
    }

    public DiscoveryNode node() {
        return node;
    }

    @Override
    public String toString() {
        return "Allocate{bucket=" + bucket + ", " + node + '}';
    }
}
