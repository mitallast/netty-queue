package org.mitallast.queue.raft.protocol;

import javaslang.collection.Vector;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;

public class RaftSnapshot implements Message {
    public static final Codec<RaftSnapshot> codec = Codec.of(
        RaftSnapshot::new,
        RaftSnapshot::getMeta,
        RaftSnapshot::getData,
        RaftSnapshotMetadata.codec,
        Codec.vectorCodec(Codec.anyCodec())
    );

    private final RaftSnapshotMetadata meta;
    private final Vector<Message> data;

    public RaftSnapshot(RaftSnapshotMetadata meta, Vector<Message> data) {
        this.meta = meta;
        this.data = data;
    }

    public RaftSnapshotMetadata getMeta() {
        return meta;
    }

    public Vector<Message> getData() {
        return data;
    }

    public LogEntry toEntry() {
        return new LogEntry(meta.getLastIncludedTerm(), meta.getLastIncludedIndex(), 0, this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RaftSnapshot that = (RaftSnapshot) o;

        if (!meta.equals(that.meta)) return false;
        return data.equals(that.data);
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
