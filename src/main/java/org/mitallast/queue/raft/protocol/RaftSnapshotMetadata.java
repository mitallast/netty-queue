package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.cluster.ClusterConfiguration;
import org.mitallast.queue.raft.Term;

import java.io.IOException;

public class RaftSnapshotMetadata implements Streamable {
    private final Term lastIncludedTerm;
    private final long lastIncludedIndex;
    private final ClusterConfiguration config;

    public RaftSnapshotMetadata(StreamInput stream) throws IOException {
        lastIncludedTerm = new Term(stream.readLong());
        lastIncludedIndex = stream.readLong();
        config = stream.readStreamable();
    }

    public RaftSnapshotMetadata(Term lastIncludedTerm, long lastIncludedIndex, ClusterConfiguration config) {
        this.lastIncludedTerm = lastIncludedTerm;
        this.lastIncludedIndex = lastIncludedIndex;
        this.config = config;
    }

    public Term getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    public long getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    public ClusterConfiguration getConfig() {
        return config;
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeLong(lastIncludedTerm.getTerm());
        stream.writeLong(lastIncludedIndex);
        stream.writeClass(config.getClass());
        stream.writeStreamable(config);
    }
}
