package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

public class ClientMessage implements Streamable {
    private final DiscoveryNode client;
    private final Streamable cmd;

    public ClientMessage(DiscoveryNode client, Streamable cmd) {
        this.client = client;
        this.cmd = cmd;
    }

    public ClientMessage(StreamInput stream) {
        client = stream.readStreamable(DiscoveryNode::new);
        cmd = stream.readStreamable();
    }

    @Override
    public void writeTo(StreamOutput stream) {
        stream.writeStreamable(client);
        stream.writeClass(cmd.getClass());
        stream.writeStreamable(cmd);
    }

    public DiscoveryNode getClient() {
        return client;
    }

    public Streamable getCmd() {
        return cmd;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClientMessage that = (ClientMessage) o;

        if (!client.equals(that.client)) return false;
        return cmd.equals(that.cmd);

    }

    @Override
    public int hashCode() {
        int result = client.hashCode();
        result = 31 * result + cmd.hashCode();
        return result;
    }
}
