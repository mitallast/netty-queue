package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft.RaftMessage;
import org.mitallast.queue.raft.Term;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class AppendRejected implements RaftMessage {
    private final DiscoveryNode member;
    private final Term term;

    public AppendRejected(StreamInput stream) throws IOException {
        member = stream.readStreamable(DiscoveryNode::new);
        term = new Term(stream.readLong());
    }

    public AppendRejected(DiscoveryNode member, Term term) {
        this.member = member;
        this.term = term;
    }

    public DiscoveryNode getMember() {
        return member;
    }

    public Term getTerm() {
        return term;
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeStreamable(member);
        stream.writeLong(term.getTerm());
    }
}
