package org.mitallast.queue.crdt.protocol;

import com.google.common.collect.ImmutableList;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.crdt.log.LogEntry;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class AppendEntries implements Streamable {
    private final int bucket;
    private final DiscoveryNode member;
    private final long prevVclock;
    private final ImmutableList<LogEntry> entries;

    public AppendEntries(int bucket, DiscoveryNode member, long prevVclock, ImmutableList<LogEntry> entries) {
        this.bucket = bucket;
        this.member = member;
        this.prevVclock = prevVclock;
        this.entries = entries;
    }

    public AppendEntries(StreamInput stream) throws IOException {
        this.bucket = stream.readInt();
        this.member = stream.readStreamable(DiscoveryNode::new);
        this.prevVclock = stream.readLong();
        this.entries = stream.readStreamableList(LogEntry::new);
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeInt(bucket);
        stream.writeStreamable(member);
        stream.writeLong(prevVclock);
        stream.writeStreamableList(entries);
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

    public ImmutableList<LogEntry> entries() {
        return entries;
    }
}
