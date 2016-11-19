package org.mitallast.queue.blob.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class PutBlobResourceRequest implements Streamable {
    private final DiscoveryNode node;
    private final long id;
    private final String key;
    private final byte[] data;

    public PutBlobResourceRequest(DiscoveryNode node, long id, String key, byte[] data) {
        this.node = node;
        this.id = id;
        this.key = key;
        this.data = data;
    }

    public PutBlobResourceRequest(StreamInput stream) throws IOException {
        node = stream.readStreamable(DiscoveryNode::new);
        id = stream.readLong();
        key = stream.readText();
        int size = stream.readInt();
        byte[] bytes = new byte[size];
        stream.readFully(bytes);
        data = bytes;
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

    public byte[] getData() {
        return data;
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeStreamable(node);
        stream.writeLong(id);
        stream.writeText(key);
        stream.writeInt(data.length);
        stream.write(data);
    }
}
