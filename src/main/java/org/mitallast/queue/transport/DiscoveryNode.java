package org.mitallast.queue.transport;

import com.google.common.net.HostAndPort;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;

public class DiscoveryNode implements Streamable {
    private final HostAndPort address;

    public DiscoveryNode(StreamInput stream) throws IOException {
        address = HostAndPort.fromParts(stream.readText(), stream.readInt());
    }

    public DiscoveryNode(HostAndPort address) {
        this.address = address;
    }

    public HostAndPort address() {
        return address;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DiscoveryNode that = (DiscoveryNode) o;

        return address.equals(that.address);
    }

    @Override
    public int hashCode() {
        return address.hashCode();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeText(address.getHostText());
        stream.writeInt(address.getPort());
    }

    @Override
    public String toString() {
        return "DiscoveryNode{" + address + '}';
    }
}
