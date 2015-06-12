package org.mitallast.queue.transport;

import org.mitallast.queue.Version;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;
import java.util.UUID;

public class DiscoveryNode implements Streamable {

    private String name;
    private UUID nodeId;
    private String host;
    private int port;
    private Version version = Version.CURRENT;

    public DiscoveryNode() {
    }

    public DiscoveryNode(String name, UUID nodeId, String host, int port, Version version) {
        this.name = name;
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.version = version;
    }

    public String nodeName() {
        return name;
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

    public Version getVersion() {
        return version;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        name = stream.readText();
        nodeId = stream.readUUID();
        host = stream.readText();
        port = stream.readInt();
        version = Version.fromId(stream.readInt());
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeText(name);
        stream.writeUUID(nodeId);
        stream.writeText(host);
        stream.writeInt(port);
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
            "name=" + name +
            ", id=" + nodeId.toString().substring(0, 8) +
            ", tcp://" + host + ':' + port +
            '}';
    }
}
