package org.mitallast.queue.raft2.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft2.RaftMessage;
import org.mitallast.queue.raft2.Term;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class InstallSnapshot implements RaftMessage {
    private DiscoveryNode leader;
    private Term term;
    private RaftSnapshot snapshot;

    protected InstallSnapshot() {
    }

    public InstallSnapshot(DiscoveryNode leader, Term term, RaftSnapshot snapshot) {
        this.leader = leader;
        this.term = term;
        this.snapshot = snapshot;
    }

    public DiscoveryNode getLeader() {
        return leader;
    }

    public Term getTerm() {
        return term;
    }

    public RaftSnapshot getSnapshot() {
        return snapshot;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        leader = stream.readStreamable(DiscoveryNode::new);
        term = new Term(stream.readLong());
        snapshot = stream.readStreamable(RaftSnapshot::new);
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeStreamable(leader);
        stream.writeLong(term.getTerm());
        stream.writeStreamable(snapshot);
    }
}
