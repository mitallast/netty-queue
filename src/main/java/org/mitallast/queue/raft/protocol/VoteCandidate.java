package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.Term;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class VoteCandidate implements Streamable {
    private final DiscoveryNode member;
    private final Term term;

    public VoteCandidate(StreamInput stream) throws IOException {
        member = stream.readStreamable(DiscoveryNode::new);
        term = new Term(stream.readLong());
    }

    public VoteCandidate(DiscoveryNode member, Term term) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VoteCandidate that = (VoteCandidate) o;

        if (!member.equals(that.member)) return false;
        return term.equals(that.term);
    }

    @Override
    public int hashCode() {
        int result = member.hashCode();
        result = 31 * result + term.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "VoteCandidate{" +
            "member=" + member +
            ", term=" + term +
            '}';
    }
}
