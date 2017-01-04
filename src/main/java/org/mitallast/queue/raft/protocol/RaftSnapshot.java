package org.mitallast.queue.raft.protocol;

import com.google.common.collect.ImmutableList;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class RaftSnapshot implements Streamable {
    private final RaftSnapshotMetadata meta;
    private final ImmutableList<Streamable> data;

    public RaftSnapshot(RaftSnapshotMetadata meta, ImmutableList<Streamable> data) {
        this.meta = meta;
        this.data = data;
    }

    public RaftSnapshot(StreamInput stream) throws IOException {
        meta = stream.readStreamable(RaftSnapshotMetadata::new);
        int size = stream.readInt();
        ImmutableList.Builder<Streamable> builder = ImmutableList.builder();
        for (int i = 0; i < size; i++) {
            Streamable streamable = stream.readStreamable();
            builder.add(streamable);
        }
        data = builder.build();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeStreamable(meta);
        stream.writeInt(data.size());
        for (Streamable item : data) {
            stream.writeClass(item.getClass());
            stream.writeStreamable(item);
        }
    }

    public RaftSnapshotMetadata getMeta() {
        return meta;
    }

    public ImmutableList<Streamable> getData() {
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
