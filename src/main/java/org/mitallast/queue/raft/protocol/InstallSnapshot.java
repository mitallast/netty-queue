package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.transport.DiscoveryNode;

public class InstallSnapshot implements Message {
    public static final Codec<InstallSnapshot> codec = Codec.Companion.of(
        InstallSnapshot::new,
        InstallSnapshot::getLeader,
        InstallSnapshot::getTerm,
        InstallSnapshot::getSnapshot,
        DiscoveryNode.codec,
        Codec.Companion.longCodec(),
        RaftSnapshot.codec
    );

    private final DiscoveryNode leader;
    private final long term;
    private final RaftSnapshot snapshot;

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
