package org.mitallast.queue.crdt.log;

import javaslang.collection.Vector;
import org.mitallast.queue.common.stream.Streamable;

import java.io.Closeable;

public interface ReplicatedLog extends Closeable {

    long vclock();

    LogEntry append(long id, Streamable event);

    Vector<LogEntry> entriesFrom(long nodeVclock);

    void delete();

    @Override
    void close();
}
