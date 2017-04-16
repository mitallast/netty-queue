package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

public class InstallSnapshot implements Streamable {
    private final DiscoveryNode leader;
    private final long term;
    private final RaftSnapshot snapshot;

    public InstallSnapshot(StreamInput stream) {
        leader = stream.readStreamable(DiscoveryNode::new);
        term = stream.readLong();
        snapshot = stream.readStreamable(RaftSnapshot::new);
    }

    public InstallSnapshot(DiscoveryNode leader, long term, RaftSnapshot snapshot) {
        this.leader = leader;
        this.term = term;
        this.snapshot = snapshot;
    }

    public DiscoveryNode getLeader() {
        return leader;
    }

    public long getTerm() {
        return term;
    }

    public RaftSnapshot getSnapshot() {
        return snapshot;
    }

    @Override
    public void writeTo(StreamOutput stream) {
        stream.writeStreamable(leader);
        stream.writeLong(term);
        stream.writeStreamable(snapshot);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InstallSnapshot that = (InstallSnapshot) o;

        if (term != that.term) return false;
        if (!leader.equals(that.leader)) return false;
        return snapshot.equals(that.snapshot);
    }

    @Override
    public int hashCode() {
        int result = leader.hashCode();
        result = 31 * result + (int) (term ^ (term >>> 32));
        result = 31 * result + snapshot.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "InstallSnapshot{" +
            "leader=" + leader +
            ", term=" + term +
            ", snapshot=" + snapshot +
            '}';
    }
}
