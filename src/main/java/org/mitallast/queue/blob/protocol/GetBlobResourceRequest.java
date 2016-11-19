package org.mitallast.queue.blob.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class GetBlobResourceRequest implements Streamable {
    private final DiscoveryNode node;
    private final long id;
    private final String key;

    public GetBlobResourceRequest(DiscoveryNode node, long id, String key) {
        this.node = node;
        this.id = id;
        this.key = key;
    }

    public GetBlobResourceRequest(StreamInput stream) throws IOException {
        node = stream.readStreamable(DiscoveryNode::new);
        id = stream.readLong();
        key = stream.readText();
    }

    public DiscoveryNode getNode() {
        return node;
    }

    public long getId() {
        return id;
    }

    public String getKey() {
        return key;
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeStreamable(node);
        stream.writeLong(id);
        stream.writeText(key);
    }
}
