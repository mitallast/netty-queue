package org.mitallast.queue.crdt.log;

import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;

public class LogEntry implements Message {
    public static final Codec<LogEntry> codec = Codec.Companion.of(
        LogEntry::new,
        LogEntry::index,
        LogEntry::id,
        LogEntry::event,
        Codec.Companion.longCodec(),
        Codec.Companion.longCodec(),
        Codec.Companion.anyCodec()
    );

    private final long index;
    private final long id;
    private final Message event;

    public LogEntry(long index, long id, Message event) {
        this.index = index;
        this.id = id;
        this.event = event;
    }

    public long index() {
        return index;
    }

    public long id() {
        return id;
    }

    public Message event() {
        return event;
    }

    @Override
    public String toString() {
        return "LogEntry{" +
            "index=" + index +
            ", id=" + id +
            ", event=" + event +
            '}';
    }
}
