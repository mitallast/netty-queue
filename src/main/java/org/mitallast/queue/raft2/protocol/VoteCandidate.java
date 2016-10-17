package org.mitallast.queue.raft2.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft2.RaftMessage;
import org.mitallast.queue.raft2.Term;

import java.io.IOException;

public class VoteCandidate implements RaftMessage {
    private Term term;

    protected VoteCandidate() {
    }

    public VoteCandidate(Term term) {
        this.term = term;
    }

    public Term getTerm() {
        return term;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        term = new Term(stream.readLong());
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeLong(term.getTerm());
    }
}
