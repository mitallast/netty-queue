package org.mitallast.queue.raft.log.compaction;

public interface MajorCompactionFactory {

    MajorCompaction create(long index);
}
