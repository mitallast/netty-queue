package org.mitallast.queue.crdt.log;

import javaslang.collection.Vector;
import org.mitallast.queue.common.codec.Message;

import java.io.Closeable;

public interface ReplicatedLog extends Closeable {

    long index();

    LogEntry append(long id, Message event);

    Vector<LogEntry> entriesFrom(long index);

    void delete();

    @Override
    void close();
}
