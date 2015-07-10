package org.mitallast.queue.raft.log.entry;

import org.mitallast.queue.raft.log.compaction.Compaction;

@FunctionalInterface
public interface EntryFilter {

    boolean accept(LogEntry entry, Compaction compaction);
}
