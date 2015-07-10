package org.mitallast.queue.raft.log.compaction;

public interface MinorCompactionFactory {

    MinorCompaction create(long index);
}
