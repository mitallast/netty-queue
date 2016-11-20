package org.mitallast.queue.raft.cluster;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class JointRequest implements Streamable {
    private final DiscoveryNode member;

    public JointRequest(StreamInput stream) throws IOException {
        member = stream.readStreamable(DiscoveryNode::new);
    }

    public JointRequest(DiscoveryNode member) {
        this.member = member;
    }

    public DiscoveryNode member() {
        return member;
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeStreamable(member);
    }

    @Override
    public String toString() {
        return "JointRequest{" + member + '}';
    }
}
