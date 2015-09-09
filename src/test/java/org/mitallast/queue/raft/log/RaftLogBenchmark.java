package org.mitallast.queue.raft.log;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.stream.InternalStreamService;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.raft.RaftStreamService;
import org.mitallast.queue.raft.log.entry.RaftLogEntry;

public class RaftLogBenchmark extends BaseTest {
    private RocksDBRaftLog raftLog;
    private RaftLogEntryGenerator generator = new RaftLogEntryGenerator(random);

    @Override
    protected int max() {
        return 600_000;
    }

    @Before
    public void setUp() throws Exception {
        StreamService streamService = new InternalStreamService(ImmutableSettings.EMPTY);
        new RaftStreamService(streamService);
        raftLog = new RocksDBRaftLog(ImmutableSettings.builder()
            .put("work_dir", testFolder.getRoot().toString())
            .build(), streamService);
        raftLog.start();
    }

    @After
    public void tearDown() throws Exception {
        raftLog.stop();
        raftLog.close();
    }

    @Test
    public void testAppend() throws Exception {
        RaftLogEntry[] entries = generator.generate(1000);
        for (RaftLogEntry entry : entries) {
            raftLog.appendEntry(entry);
        }
        entries = generator.generate(max(), 1000);
        long start = System.currentTimeMillis();
        for (RaftLogEntry entry : entries) {
            raftLog.appendEntry(entry);
        }
        long end = System.currentTimeMillis();
        printQps("append", max(), start, end);
    }

    @Test
    public void testGetEntry() throws Exception {
        RaftLogEntry[] entries = generator.generate(max());
        for (RaftLogEntry entry : entries) {
            raftLog.appendEntry(entry);
        }
        long start = System.currentTimeMillis();
        for (RaftLogEntry entry : entries) {
            raftLog.getEntry(entry.index());
        }
        long end = System.currentTimeMillis();
        printQps("get", max(), start, end);
    }

    @Test
    public void testContainsIndex() throws Exception {
        RaftLogEntry[] entries = generator.generate(max());
        for (RaftLogEntry entry : entries) {
            raftLog.appendEntry(entry);
        }
        long start = System.currentTimeMillis();
        for (RaftLogEntry entry : entries) {
            raftLog.containsIndex(entry.index());
        }
        long end = System.currentTimeMillis();
        printQps("contains index", max(), start, end);
    }

    @Test
    public void testContainsEntry() throws Exception {
        RaftLogEntry[] entries = generator.generate(max());
        for (RaftLogEntry entry : entries) {
            raftLog.appendEntry(entry);
        }
        long start = System.currentTimeMillis();
        for (RaftLogEntry entry : entries) {
            raftLog.containsEntry(entry.index());
        }
        long end = System.currentTimeMillis();
        printQps("contains entry", max(), start, end);
    }
}
