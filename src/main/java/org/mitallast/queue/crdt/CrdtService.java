package org.mitallast.queue.crdt;

import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.crdt.log.LogEntry;

public interface CrdtService {

    void createLWWRegister(long id);

    void update(long id, Streamable event);

    boolean shouldCompact(LogEntry logEntry);
}
