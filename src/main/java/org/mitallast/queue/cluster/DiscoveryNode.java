package org.mitallast.queue.cluster;

import com.google.common.collect.ImmutableMap;
import org.mitallast.queue.Version;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public class DiscoveryNode implements Streamable {

    private UUID nodeId;
    private String host;
    private int port;
    private ImmutableMap<String, String> attributes;
    private Version version = Version.CURRENT;

    public DiscoveryNode() {
    }

    public DiscoveryNode(UUID nodeId, String host, int port, ImmutableMap<String, String> attributes, Version version) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.attributes = attributes;
        this.version = version;
    }

    public UUID getNodeId() {
        return nodeId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public ImmutableMap<String, String> getAttributes() {
        return attributes;
    }

    public Version getVersion() {
        return version;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        nodeId = stream.readUUID();
        host = stream.readText();
        port = stream.readInt();
        int size = stream.readInt();
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (int i = 0; i < size; i++) {
            builder.put(stream.readText(), stream.readText());
        }
        attributes = builder.build();
        version = Version.fromId(stream.readInt());
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeUUID(nodeId);
        stream.writeText(host);
        stream.writeInt(port);
        stream.writeInt(attributes.size());
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            stream.writeText(entry.getKey());
            stream.writeText(entry.getValue());
        }
        stream.writeInt(version.id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DiscoveryNode that = (DiscoveryNode) o;

        return nodeId.equals(that.nodeId);

    }

    @Override
    public int hashCode() {
        return nodeId.hashCode();
    }

    @Override
    public String toString() {
        return "DiscoveryNode{" +
            "nodeId=" + nodeId +
            ", host='" + host + '\'' +
            ", port=" + port +
            ", attributes=" + attributes +
            ", version=" + version +
            '}';
    }
}
