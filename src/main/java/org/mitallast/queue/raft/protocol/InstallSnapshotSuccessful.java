package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft.RaftMessage;
import org.mitallast.queue.raft.Term;

import java.io.IOException;

public class InstallSnapshotSuccessful implements RaftMessage {
    private final Term term;
    private final long lastIndex;

    public InstallSnapshotSuccessful(StreamInput stream) throws IOException {
        term = new Term(stream.readLong());
        lastIndex = stream.readLong();
    }

    public InstallSnapshotSuccessful(Term term, long lastIndex) {
        this.term = term;
        this.lastIndex = lastIndex;
    }

    public Term getTerm() {
        return term;
    }

    public long getLastIndex() {
        return lastIndex;
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeLong(term.getTerm());
        stream.writeLong(lastIndex);
    }
}
