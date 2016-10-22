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
}
