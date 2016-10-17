package org.mitallast.queue.raft2.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft2.RaftMessage;
import org.mitallast.queue.raft2.Term;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class RequestVote implements RaftMessage {
    private Term term;
    private DiscoveryNode candidate;
    private Term lastLogTerm;
    private long lastLogIndex;

    protected RequestVote() {
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
    public void readFrom(StreamInput stream) throws IOException {
        term = new Term(stream.readLong());
        candidate = stream.readStreamable(DiscoveryNode::new);
        lastLogTerm = new Term(stream.readLong());
        lastLogIndex = stream.readLong();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeLong(term.getTerm());
        stream.writeStreamable(candidate);
        stream.writeLong(lastLogTerm.getTerm());
        stream.writeLong(lastLogIndex);
    }
}
