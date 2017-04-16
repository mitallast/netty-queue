package org.mitallast.queue.raft.protocol;

import javaslang.collection.Vector;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

public class RaftSnapshot implements Streamable {
    private final RaftSnapshotMetadata meta;
    private final Vector<Streamable> data;

    public RaftSnapshot(RaftSnapshotMetadata meta, Vector<Streamable> data) {
        this.meta = meta;
        this.data = data;
    }

    public RaftSnapshot(StreamInput stream) {
        meta = stream.readStreamable(RaftSnapshotMetadata::new);
        data = stream.readVector();
    }

    @Override
    public void writeTo(StreamOutput stream) {
        stream.writeStreamable(meta);
        stream.writeTypedVector(data);
    }

    public RaftSnapshotMetadata getMeta() {
        return meta;
    }

    public Vector<Streamable> getData() {
        return data;
    }

    public LogEntry toEntry(DiscoveryNode node) {
        return new LogEntry(this, meta.getLastIncludedTerm(), meta.getLastIncludedIndex(), node);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RaftSnapshot that = (RaftSnapshot) o;

        return meta.equals(that.meta) && data.equals(that.data);
    }

    @Override
    public int hashCode() {
        int result = meta.hashCode();
        result = 31 * result + data.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "RaftSnapshot{" +
            "meta=" + meta +
            ", data=" + data +
            '}';
    }
}
