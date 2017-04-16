package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

public class RemoveServer implements Streamable {
    private final DiscoveryNode member;

    public RemoveServer(DiscoveryNode member) {
        this.member = member;
    }

    public RemoveServer(StreamInput stream) {
        member = stream.readStreamable(DiscoveryNode::new);
    }

    public void writeTo(StreamOutput stream) {
        stream.writeStreamable(member);
    }

    public DiscoveryNode getMember() {
        return member;
    }
}