package org.mitallast.queue.raft.log.entry;

import org.mitallast.queue.raft.log.Compaction;

@FunctionalInterface
public interface EntryFilter {

    boolean accept(LogEntry entry, Compaction compaction);
}
