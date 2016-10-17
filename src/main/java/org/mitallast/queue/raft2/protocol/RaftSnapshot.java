package org.mitallast.queue.raft2.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft2.RaftMessage;

import java.io.IOException;
import java.util.Optional;

public class RaftSnapshot implements RaftMessage {
    private RaftSnapshotMetadata meta;
    private Streamable data;

    protected RaftSnapshot() {
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
    public void readFrom(StreamInput stream) throws IOException {
        meta = stream.readStreamable(RaftSnapshotMetadata::new);
        data = stream.readStreamable();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeStreamable(meta);
        stream.writeClass(data.getClass());
        stream.writeStreamable(data);
    }
}
