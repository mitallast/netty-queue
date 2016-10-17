package org.mitallast.queue.raft2.cluster;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft2.RaftMessage;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class RaftMemberRemoved implements RaftMessage {
    private DiscoveryNode member;
    private int keepInitUntil;

    protected RaftMemberRemoved() {
    }

    public RaftMemberRemoved(DiscoveryNode member, int keepInitUntil) {
        this.member = member;
        this.keepInitUntil = keepInitUntil;
    }

    public DiscoveryNode getMember() {
        return member;
    }

    public int getKeepInitUntil() {
        return keepInitUntil;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        member = stream.readStreamable(DiscoveryNode::new);
        keepInitUntil = stream.readInt();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeStreamable(member);
        stream.writeInt(keepInitUntil);
    }
}
