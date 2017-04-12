package org.mitallast.queue.crdt.log;

import java.util.function.Predicate;

public interface ReplicatedLogFactory {
    ReplicatedLog create(int index, Predicate<LogEntry> compactionFilter);
}
