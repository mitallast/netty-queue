package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

public class AddServer implements Streamable {
    private final DiscoveryNode member;

    public AddServer(StreamInput stream) {
        member = stream.readStreamable(DiscoveryNode::new);
    }

    public AddServer(DiscoveryNode member) {
        this.member = member;
    }

    public DiscoveryNode getMember() {
        return member;
    }

    public void writeTo(StreamOutput stream) {
        stream.writeStreamable(member);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AddServer addServer = (AddServer) o;

        return member.equals(addServer.member);
    }

    @Override
    public int hashCode() {
        return member.hashCode();
    }

    @Override
    public String toString() {
        return "AddServer{" + member + "}";
    }
}