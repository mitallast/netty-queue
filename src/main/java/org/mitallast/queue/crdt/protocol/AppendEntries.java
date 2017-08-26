package org.mitallast.queue.crdt.protocol;

import javaslang.collection.Vector;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.crdt.log.LogEntry;

public class AppendEntries implements Message {
    public static final Codec<AppendEntries> codec = Codec.of(
        AppendEntries::new,
        AppendEntries::bucket,
        AppendEntries::replica,
        AppendEntries::prevIndex,
        AppendEntries::entries,
        Codec.intCodec,
        Codec.longCodec,
        Codec.longCodec,
        Codec.vectorCodec(LogEntry.codec)
    );

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
