package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.Term;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class RequestVote implements Streamable {
    private final Term term;
    private final DiscoveryNode candidate;
    private final Term lastLogTerm;
    private final long lastLogIndex;

    public RequestVote(StreamInput stream) throws IOException {
        term = new Term(stream.readLong());
        candidate = stream.readStreamable(DiscoveryNode::new);
        lastLogTerm = new Term(stream.readLong());
        lastLogIndex = stream.readLong();
    }

    public RequestVote(Term term, DiscoveryNode candidate, Term lastLogTerm, long lastLogIndex) {
        this.term = term;
        this.candidate = candidate;
        this.lastLogTerm = lastLogTerm;
        this.lastLogIndex = lastLogIndex;
    }

    public Term getTerm() {
        return term;
    }

    public DiscoveryNode getCandidate() {
        return candidate;
    }

    public Term getLastLogTerm() {
        return lastLogTerm;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeLong(term.getTerm());
        stream.writeStreamable(candidate);
        stream.writeLong(lastLogTerm.getTerm());
        stream.writeLong(lastLogIndex);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RequestVote that = (RequestVote) o;

        if (lastLogIndex != that.lastLogIndex) return false;
        if (!term.equals(that.term)) return false;
        if (!candidate.equals(that.candidate)) return false;
        return lastLogTerm.equals(that.lastLogTerm);

    }

    @Override
    public int hashCode() {
        int result = term.hashCode();
        result = 31 * result + candidate.hashCode();
        result = 31 * result + lastLogTerm.hashCode();
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
