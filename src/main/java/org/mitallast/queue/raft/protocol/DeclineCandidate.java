package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

public class DeclineCandidate implements Streamable {
    private final DiscoveryNode member;
    private final long term;

    public DeclineCandidate(StreamInput stream) {
        member = stream.readStreamable(DiscoveryNode::new);
        term = stream.readLong();
    }

    public DeclineCandidate(DiscoveryNode member, long term) {
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
    public void writeTo(StreamOutput stream) {
        stream.writeStreamable(member);
        stream.writeLong(term);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DeclineCandidate that = (DeclineCandidate) o;

        if (!member.equals(that.member)) return false;
        return term == that.term;
    }

    @Override
    public int hashCode() {
        int result = member.hashCode();
        result = 31 * result + (int) (term ^ (term >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "DeclineCandidate{" +
            "member=" + member +
            ", term=" + term +
            '}';
    }
}
