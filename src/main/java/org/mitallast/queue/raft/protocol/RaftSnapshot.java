package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;
import java.util.Optional;

public class RaftSnapshot implements Streamable {
    private final RaftSnapshotMetadata meta;
    private final Streamable data;

    public RaftSnapshot(StreamInput stream) throws IOException {
        meta = stream.readStreamable(RaftSnapshotMetadata::new);
        data = stream.readStreamable();
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
        stream.writeClass(data.getClass());
        stream.writeStreamable(data);
    }
}
