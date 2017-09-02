package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.transport.DiscoveryNode;

public class InstallSnapshotRejected implements Message {
    public static final Codec<InstallSnapshotRejected> codec = Codec.Companion.of(
        InstallSnapshotRejected::new,
        InstallSnapshotRejected::getMember,
        InstallSnapshotRejected::getTerm,
        DiscoveryNode.codec,
        Codec.Companion.longCodec()
    );

    private final DiscoveryNode member;
    private final long term;

    public InstallSnapshotRejected(DiscoveryNode member, long term) {
        this.member = member;
        this.term = term;
    }

    public DiscoveryNode getMember() {
        return member;
    }

    public long getTerm() {
        return term;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InstallSnapshotRejected that = (InstallSnapshotRejected) o;

        if (term != that.term) return false;
        return member.equals(that.member);
    }

    @Override
    public int hashCode() {
        int result = member.hashCode();
        result = 31 * result + (int) (term ^ (term >>> 32));
        return result;
    }
}
