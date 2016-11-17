package org.mitallast.queue.transport;

import com.google.common.base.Preconditions;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;

public class DiscoveryNode implements Streamable {
    private final String host;
    private final int port;

    public DiscoveryNode(StreamInput stream) throws IOException {
        host = stream.readText();
        port = stream.readInt();
    }

    public DiscoveryNode(String host, int port) {
        Preconditions.checkNotNull(host);
        this.host = host;
        this.port = port;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeText(host);
        stream.writeInt(port);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DiscoveryNode that = (DiscoveryNode) o;

        return port == that.port && host.equals(that.host);
    }

    @Override
    public int hashCode() {
        int result = host.hashCode();
        result = 31 * result + port;
        return result;
    }

    @Override
    public String toString() {
        return "DiscoveryNode{host=" + host + ", port=" + port + '}';
    }
}
