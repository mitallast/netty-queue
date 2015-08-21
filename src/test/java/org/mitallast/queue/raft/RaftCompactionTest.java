package org.mitallast.queue.raft;

import org.junit.Test;
import org.mitallast.queue.raft.log.RaftLog;
import org.mitallast.queue.raft.log.compaction.Compactor;
import org.mitallast.queue.raft.log.entry.RaftLogEntry;
import org.mitallast.queue.raft.util.ExecutionContext;

public class RaftCompactionTest extends BaseRaftTest {

    @Test
    public void testCompaction() throws Exception {
        printLog();
        Compactor compactor = leader.injector().getInstance(Compactor.class);
        compactor.compact().get();
        printLog();
    }

    private void printLog() throws Exception {
        ExecutionContext executionContext = leader.injector().getInstance(ExecutionContext.class);
        executionContext.submit(() -> {
            try {
                RaftLog raftLog = leader.injector().getInstance(RaftLog.class);
                long firstIndex = raftLog.firstIndex();
                long lastIndex = raftLog.lastIndex();
                for (long index = firstIndex; index <= lastIndex; index++) {
                    RaftLogEntry entry = raftLog.getEntry(index);
                    logger.info("index {} entry {}", index, entry);
                }
            } catch (Throwable e) {
                logger.error("error", e);
            }
        }).get();
    }
}
