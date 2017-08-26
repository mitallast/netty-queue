package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.transport.DiscoveryNode;

public class RequestVote implements Message {
    public static final Codec<RequestVote> codec = Codec.of(
        RequestVote::new,
        RequestVote::getTerm,
        RequestVote::getCandidate,
        RequestVote::getLastLogTerm,
        RequestVote::getLastLogIndex,
        Codec.longCodec,
        DiscoveryNode.codec,
        Codec.longCodec,
        Codec.longCodec
    );

    private final long term;
    private final DiscoveryNode candidate;
    private final long lastLogTerm;
    private final long lastLogIndex;

    public RequestVote(long term, DiscoveryNode candidate, long lastLogTerm, long lastLogIndex) {
        this.term = term;
        this.candidate = candidate;
        this.lastLogTerm = lastLogTerm;
        this.lastLogIndex = lastLogIndex;
    }

    public long getTerm() {
        return term;
    }

    public DiscoveryNode getCandidate() {
        return candidate;
    }

    public long getLastLogTerm() {
        return lastLogTerm;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RequestVote that = (RequestVote) o;

        if (term != that.term) return false;
        if (lastLogTerm != that.lastLogTerm) return false;
        if (lastLogIndex != that.lastLogIndex) return false;
        return candidate.equals(that.candidate);
    }

    @Override
    public int hashCode() {
        int result = (int) (term ^ (term >>> 32));
        result = 31 * result + candidate.hashCode();
        result = 31 * result + (int) (lastLogTerm ^ (lastLogTerm >>> 32));
        result = 31 * result + (int) (lastLogIndex ^ (lastLogIndex >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "RequestVote{" +
            "term=" + term +
            ", candidate=" + candidate +
            ", lastLogTerm=" + lastLogTerm +
            ", lastLogIndex=" + lastLogIndex +
            '}';
    }
}
