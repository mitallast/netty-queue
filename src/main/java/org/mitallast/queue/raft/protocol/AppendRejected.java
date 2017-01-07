package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class AppendRejected implements Streamable {
    private final DiscoveryNode member;
    private final long term;
    private final long lastIndex;

    public AppendRejected(StreamInput stream) throws IOException {
        member = stream.readStreamable(DiscoveryNode::new);
        term = stream.readLong();
        lastIndex = stream.readLong();
    }

    public AppendRejected(DiscoveryNode member, long term, long lastIndex) {
        this.member = member;
        this.term = term;
        this.lastIndex = lastIndex;
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeStreamable(member);
        stream.writeLong(term);
        stream.writeLong(lastIndex);
    }

    public DiscoveryNode getMember() {
        return member;
    }

    public long getTerm() {
        return term;
    }

    public long getLastIndex() {
        return lastIndex;
    }
}
