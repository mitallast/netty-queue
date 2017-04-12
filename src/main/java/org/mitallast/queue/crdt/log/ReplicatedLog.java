package org.mitallast.queue.crdt.log;

import com.google.common.collect.ImmutableList;
import org.mitallast.queue.common.stream.Streamable;

import java.io.Closeable;
import java.io.IOException;

public interface ReplicatedLog extends Closeable {

    LogEntry append(long id, Streamable event) throws IOException;

    ImmutableList<LogEntry> entriesFrom(long nodeVclock);

    void delete() throws IOException;
}
