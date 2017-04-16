package org.mitallast.queue.crdt.protocol;

import javaslang.collection.Vector;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.crdt.log.LogEntry;
import org.mitallast.queue.transport.DiscoveryNode;

public class AppendEntries implements Streamable {
    private final int bucket;
    private final DiscoveryNode member;
    private final long prevVclock;
    private final Vector<LogEntry> entries;

    public AppendEntries(int bucket, DiscoveryNode member, long prevVclock, Vector<LogEntry> entries) {
        this.bucket = bucket;
        this.member = member;
        this.prevVclock = prevVclock;
        this.entries = entries;
    }

    public AppendEntries(StreamInput stream) {
        this.bucket = stream.readInt();
        this.member = stream.readStreamable(DiscoveryNode::new);
        this.prevVclock = stream.readLong();
        this.entries = stream.readVector(LogEntry::new);
    }

    @Override
    public void writeTo(StreamOutput stream) {
        stream.writeInt(bucket);
        stream.writeStreamable(member);
        stream.writeLong(prevVclock);
        stream.writeVector(entries);
    }

    public int bucket() {
        return bucket;
    }

    public DiscoveryNode member() {
        return member;
    }

    public long prevVclock() {
        return prevVclock;
    }

    public Vector<LogEntry> entries() {
        return entries;
    }
}
