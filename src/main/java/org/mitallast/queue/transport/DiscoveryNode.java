package org.mitallast.queue.transport;

import com.google.common.net.HostAndPort;
import org.mitallast.queue.Version;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;

public class DiscoveryNode implements Streamable {

    private String name;
    private HostAndPort address;
    private Version version = Version.CURRENT;

    public DiscoveryNode() {
    }

    public DiscoveryNode(String name, HostAndPort address, Version version) {
        this.name = name;
        this.address = address;
        this.version = version;
    }

    public String name() {
        return name;
    }

    public HostAndPort address() {
        return address;
    }

    public Version version() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DiscoveryNode that = (DiscoveryNode) o;

        return address.equals(that.address) && version.equals(that.version);
    }

    @Override
    public int hashCode() {
        int result = address.hashCode();
        result = 31 * result + version.hashCode();
        return result;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        name = stream.readText();
        address = HostAndPort.fromParts(stream.readText(), stream.readInt());
        version = Version.fromId(stream.readInt());
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeText(name);
        stream.writeText(address.getHostText());
        stream.writeInt(address.getPort());
        stream.writeInt(version.id);
    }

    @Override
    public String toString() {
        return "DiscoveryNode{" +
            "name=" + name +
            " , address=" + address +
            '}';
    }
}
