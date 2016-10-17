package org.mitallast.queue.raft.cluster;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft.RaftMessage;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class RaftMemberAdded implements RaftMessage {
    private final DiscoveryNode member;
    private final int keepInitUntil;

    public RaftMemberAdded(StreamInput stream) throws IOException {
        member = stream.readStreamable(DiscoveryNode::new);
        keepInitUntil = stream.readInt();
    }

    public RaftMemberAdded(DiscoveryNode member, int keepInitUntil) {
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
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeStreamable(member);
        stream.writeInt(keepInitUntil);
    }
}
