package org.mitallast.queue.blob.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class PutBlobResource implements Streamable {
    private final String key;
    private final DiscoveryNode node;

    public PutBlobResource(String key, DiscoveryNode node) {
        this.key = key;
        this.node = node;
    }

    public PutBlobResource(StreamInput stream) throws IOException {
        key = stream.readText();
        node = stream.readStreamable(DiscoveryNode::new);
    }

    public String getKey() {
        return key;
    }

    public DiscoveryNode getNode() {
        return node;
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeText(key);
        stream.writeStreamable(node);
    }
}
