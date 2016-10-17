package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft.RaftMessage;
import org.mitallast.queue.raft.Term;

import java.io.IOException;

public class InstallSnapshotRejected implements RaftMessage {
    private final Term term;

    public InstallSnapshotRejected(StreamInput stream) throws IOException {
        term = new Term(stream.readLong());
    }

    public InstallSnapshotRejected(Term term) {
        this.term = term;
    }

    public Term getTerm() {
        return term;
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeLong(term.getTerm());
    }
}
