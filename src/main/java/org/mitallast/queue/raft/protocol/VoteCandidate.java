package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class VoteCandidate implements Streamable {
    private final DiscoveryNode member;
    private final long term;

    public VoteCandidate(StreamInput stream) throws IOException {
        member = stream.readStreamable(DiscoveryNode::new);
        term = stream.readLong();
    }

    public VoteCandidate(DiscoveryNode member, long term) {
        this.member = member;
        this.term = term;
    }

    public DiscoveryNode getMember() {
        return member;
    }

    public long getTerm() {
        return term;
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeStreamable(member);
        stream.writeLong(term);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VoteCandidate that = (VoteCandidate) o;

        if (term != that.term) return false;
        return member.equals(that.member);
    }

    @Override
    public int hashCode() {
        int result = member.hashCode();
        result = 31 * result + (int) (term ^ (term >>> 32));
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
