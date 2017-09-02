package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.transport.DiscoveryNode;

public class VoteCandidate implements Message {
    public static final Codec<VoteCandidate> codec = Codec.Companion.of(
        VoteCandidate::new,
        VoteCandidate::getMember,
        VoteCandidate::getTerm,
        DiscoveryNode.codec,
        Codec.Companion.longCodec()
    );

    private final DiscoveryNode member;
    private final long term;

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
