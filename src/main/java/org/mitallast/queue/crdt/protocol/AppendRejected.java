package org.mitallast.queue.crdt.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class AppendRejected implements Streamable {
    private final DiscoveryNode member;
    private final long vclock;

    public AppendRejected(StreamInput stream) throws IOException {
        member = stream.readStreamable(DiscoveryNode::new);
        vclock = stream.readLong();
    }

    public AppendRejected(DiscoveryNode member, long vclock) {
        this.member = member;
        this.vclock = vclock;
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeStreamable(member);
        stream.writeLong(vclock);
    }

    public DiscoveryNode member() {
        return member;
    }

    public long vclock() {
        return vclock;
    }
}
