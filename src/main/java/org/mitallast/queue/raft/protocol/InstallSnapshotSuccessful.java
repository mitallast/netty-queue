package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class InstallSnapshotSuccessful implements Streamable {
    private final DiscoveryNode member;
    private final long term;
    private final long lastIndex;

    public InstallSnapshotSuccessful(StreamInput stream) throws IOException {
        member = stream.readStreamable(DiscoveryNode::new);
        term = stream.readLong();
        lastIndex = stream.readLong();
    }

    public InstallSnapshotSuccessful(DiscoveryNode member, long term, long lastIndex) {
        this.member = member;
        this.term = term;
        this.lastIndex = lastIndex;
    }

    public DiscoveryNode getMember() {
        return member;
    }

    public long getTerm() {
        return term;
    }

    public long getLastIndex() {
        return lastIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InstallSnapshotSuccessful that = (InstallSnapshotSuccessful) o;

        if (term != that.term) return false;
        if (lastIndex != that.lastIndex) return false;
        return member.equals(that.member);
    }

    @Override
    public int hashCode() {
        int result = member.hashCode();
        result = 31 * result + (int) (term ^ (term >>> 32));
        result = 31 * result + (int) (lastIndex ^ (lastIndex >>> 32));
        return result;
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeStreamable(member);
        stream.writeLong(term);
        stream.writeLong(lastIndex);
    }

    @Override
    public String toString() {
        return "InstallSnapshotSuccessful{" +
            "member=" + member +
            ", term=" + term +
            ", lastIndex=" + lastIndex +
            '}';
    }
}
