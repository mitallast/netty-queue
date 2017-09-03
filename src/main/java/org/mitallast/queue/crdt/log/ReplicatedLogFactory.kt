package org.mitallast.queue.crdt.log

import java.util.function.Predicate

interface ReplicatedLogFactory {
    fun create(index: Int, replica: Long, compactionFilter: Predicate<LogEntry>): ReplicatedLog
}
