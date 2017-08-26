package org.mitallast.queue.transport;

import com.google.common.base.Preconditions;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;

public class DiscoveryNode implements Message {
    public static final Codec<DiscoveryNode> codec = Codec.of(
        DiscoveryNode::new,
        DiscoveryNode::host,
        DiscoveryNode::port,
        Codec.stringCodec,
        Codec.intCodec
    );

    private final String host;
    private final int port;

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
        return "node{" + host + ':' + port + '}';
    }
}
