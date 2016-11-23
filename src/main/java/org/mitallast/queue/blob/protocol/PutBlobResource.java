package org.mitallast.queue.blob.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class PutBlobResource implements Streamable {
    private final DiscoveryNode node;
    private final long id;
    private final String key;

    public PutBlobResource(DiscoveryNode node, long id, String key) {
        this.node = node;
        this.id = id;
        this.key = key;
    }

    public PutBlobResource(StreamInput stream) throws IOException {
        node = stream.readStreamable(DiscoveryNode::new);
        id = stream.readLong();
        key = stream.readText();
    }

    public long getId() {
        return id;
    }

    public String getKey() {
        return key;
    }

    public DiscoveryNode getNode() {
        return node;
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeStreamable(node);
        stream.writeLong(id);
        stream.writeText(key);
    }
}
