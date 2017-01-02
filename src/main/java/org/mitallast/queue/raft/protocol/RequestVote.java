package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class RequestVote implements Streamable {
    private final long term;
    private final DiscoveryNode candidate;
    private final long lastLogTerm;
    private final long lastLogIndex;

    public RequestVote(StreamInput stream) throws IOException {
        term = stream.readLong();
        candidate = stream.readStreamable(DiscoveryNode::new);
        lastLogTerm = stream.readLong();
        lastLogIndex = stream.readLong();
    }

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
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeLong(term);
        stream.writeStreamable(candidate);
        stream.writeLong(lastLogTerm);
        stream.writeLong(lastLogIndex);
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
