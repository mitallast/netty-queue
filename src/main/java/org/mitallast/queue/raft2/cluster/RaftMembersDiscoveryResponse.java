package org.mitallast.queue.raft2.cluster;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft2.RaftMessage;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class RaftMembersDiscoveryResponse implements RaftMessage {

    private DiscoveryNode member;

    protected RaftMembersDiscoveryResponse() {
    }

    public RaftMembersDiscoveryResponse(DiscoveryNode member) {
        this.member = member;
    }

    public DiscoveryNode getMember() {
        return member;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        member = stream.readStreamable(DiscoveryNode::new);
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeStreamable(member);
    }
}
