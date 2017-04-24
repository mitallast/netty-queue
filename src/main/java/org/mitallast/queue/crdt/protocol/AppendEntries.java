package org.mitallast.queue.crdt.protocol;

import javaslang.collection.Vector;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.crdt.log.LogEntry;

public class AppendEntries implements Streamable {
    private final int bucket;
    private final long replica;
    private final long prevIndex;
    private final Vector<LogEntry> entries;

    public AppendEntries(int bucket, long replica, long prevIndex, Vector<LogEntry> entries) {
        this.bucket = bucket;
        this.replica = replica;
        this.prevIndex = prevIndex;
        this.entries = entries;
    }

    public AppendEntries(StreamInput stream) {
        this.bucket = stream.readInt();
        this.replica = stream.readLong();
        this.prevIndex = stream.readLong();
        this.entries = stream.readVector(LogEntry::new);
    }

    @Override
    public void writeTo(StreamOutput stream) {
        stream.writeInt(bucket);
        stream.writeLong(replica);
        stream.writeLong(prevIndex);
        stream.writeVector(entries);
    }

    public int bucket() {
        return bucket;
    }

    public long replica() {
        return replica;
    }

    public long prevIndex() {
        return prevIndex;
    }

    public Vector<LogEntry> entries() {
        return entries;
    }
}
