package org.mitallast.queue.raft2.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft2.RaftMessage;
import org.mitallast.queue.raft2.cluster.ClusterConfiguration;
import org.mitallast.queue.raft2.Term;

import java.io.IOException;

public class RaftSnapshotMetadata implements RaftMessage {
    private Term lastIncludedTerm;
    private long lastIncludedIndex;
    private ClusterConfiguration config;

    protected RaftSnapshotMetadata() {
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
    public void readFrom(StreamInput stream) throws IOException {
        lastIncludedTerm = new Term(stream.readLong());
        lastIncludedIndex = stream.readLong();
        config = stream.readStreamable();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeLong(lastIncludedTerm.getTerm());
        stream.writeLong(lastIncludedIndex);
        stream.writeClass(config.getClass());
        stream.writeStreamable(config);
    }
}
