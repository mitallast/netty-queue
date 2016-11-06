package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;
import java.util.Optional;

public class RaftSnapshot implements Streamable {
    private final RaftSnapshotMetadata meta;
    private final Streamable data;

    public RaftSnapshot(StreamInput stream) throws IOException {
        meta = stream.readStreamable(RaftSnapshotMetadata::new);
        if (stream.readBoolean()) {
            data = stream.readStreamable();
        } else {
            data = null;
        }
    }

    public RaftSnapshot(RaftSnapshotMetadata meta, Streamable data) {
        this.meta = meta;
        this.data = data;
    }

    public RaftSnapshotMetadata getMeta() {
        return meta;
    }

    public Streamable getData() {
        return data;
    }

    public LogEntry toEntry() {
        return new LogEntry(this, meta.getLastIncludedTerm(), meta.getLastIncludedIndex(), Optional.empty());
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeStreamable(meta);
        if (data != null) {
            stream.writeBoolean(true);
            stream.writeClass(data.getClass());
            stream.writeStreamable(data);
        } else {
            stream.writeBoolean(false);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RaftSnapshot that = (RaftSnapshot) o;

        if (!meta.equals(that.meta)) return false;
        return data != null ? data.equals(that.data) : that.data == null;

    }

    @Override
    public int hashCode() {
        int result = meta.hashCode();
        result = 31 * result + (data != null ? data.hashCode() : 0);
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
