package org.mitallast.queue.crdt.log;

import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;
import java.util.List;

public interface ReplicatedLog {

    LogEntry append(long id, Streamable event) throws IOException;

    void append(LogEntry logEntry) throws IOException;

    List<LogEntry> entries();
}
