package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft.RaftMessage;
import org.mitallast.queue.raft.Term;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class InstallSnapshot implements RaftMessage {
    private final DiscoveryNode leader;
    private final Term term;
    private final RaftSnapshot snapshot;

    public InstallSnapshot(StreamInput stream) throws IOException {
        leader = stream.readStreamable(DiscoveryNode::new);
        term = new Term(stream.readLong());
        snapshot = stream.readStreamable(RaftSnapshot::new);
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
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeStreamable(leader);
        stream.writeLong(term.getTerm());
        stream.writeStreamable(snapshot);
    }
}
