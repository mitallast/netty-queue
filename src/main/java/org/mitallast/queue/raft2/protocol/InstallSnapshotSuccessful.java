package org.mitallast.queue.raft2.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft2.RaftMessage;
import org.mitallast.queue.raft2.Term;

import java.io.IOException;

public class InstallSnapshotSuccessful implements RaftMessage {
    private Term term;
    private long lastIndex;

    protected InstallSnapshotSuccessful() {
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
    public void readFrom(StreamInput stream) throws IOException {
        term = new Term(stream.readLong());
        lastIndex = stream.readLong();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeLong(term.getTerm());
        stream.writeLong(lastIndex);
    }
}
