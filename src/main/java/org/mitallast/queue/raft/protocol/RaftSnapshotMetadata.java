package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.cluster.ClusterConfiguration;

public class RaftSnapshotMetadata implements Streamable {
    private final long lastIncludedTerm;
    private final long lastIncludedIndex;
    private final ClusterConfiguration config;

    public RaftSnapshotMetadata(long lastIncludedTerm, long lastIncludedIndex, ClusterConfiguration config) {
        this.lastIncludedTerm = lastIncludedTerm;
        this.lastIncludedIndex = lastIncludedIndex;
        this.config = config;
    }

    public RaftSnapshotMetadata(StreamInput stream) {
        lastIncludedTerm = stream.readLong();
        lastIncludedIndex = stream.readLong();
        config = stream.readStreamable();
    }

    @Override
    public void writeTo(StreamOutput stream) {
        stream.writeLong(lastIncludedTerm);
        stream.writeLong(lastIncludedIndex);
        stream.writeClass(config.getClass());
        stream.writeStreamable(config);
    }

    public long getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    public long getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    public ClusterConfiguration getConfig() {
        return config;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RaftSnapshotMetadata that = (RaftSnapshotMetadata) o;

        if (lastIncludedTerm != that.lastIncludedTerm) return false;
        if (lastIncludedIndex != that.lastIncludedIndex) return false;
        return config.equals(that.config);
    }

    @Override
    public int hashCode() {
        int result = (int) (lastIncludedTerm ^ (lastIncludedTerm >>> 32));
        result = 31 * result + (int) (lastIncludedIndex ^ (lastIncludedIndex >>> 32));
        result = 31 * result + config.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "RaftSnapshotMetadata{" +
            "lastIncludedTerm=" + lastIncludedTerm +
            ", lastIncludedIndex=" + lastIncludedIndex +
            ", config=" + config +
            '}';
    }
}
