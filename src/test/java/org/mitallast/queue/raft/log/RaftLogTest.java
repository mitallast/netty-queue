package org.mitallast.queue.raft.log;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.stream.InternalStreamService;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.raft.RaftStreamService;
import org.mitallast.queue.raft.log.entry.RaftLogEntry;
import org.unitils.reflectionassert.ReflectionAssert;

public class RaftLogTest extends BaseTest {
    private RocksDBRaftLog raftLog;

    private RaftLogEntryGenerator generator = new RaftLogEntryGenerator(random);

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
    public void testInitialState() throws Exception {
        Assert.assertEquals(0, raftLog.firstIndex());
        Assert.assertEquals(1, raftLog.nextIndex());
        Assert.assertEquals(0, raftLog.lastIndex());
    }

    @Test
    public void testAppend() throws Exception {
        RaftLogEntry[] entries = generator.generate(max());
        int index = 0;
        for (RaftLogEntry entry : entries) {
            Assert.assertEquals(index, raftLog.lastIndex());
            raftLog.appendEntry(entry);
            index++;
            Assert.assertEquals(index, raftLog.lastIndex());
        }
    }

    @Test
    public void testContainsIndex() throws Exception {
        RaftLogEntry[] entries = generator.generate(max());
        for (RaftLogEntry entry : entries) {
            raftLog.appendEntry(entry);
        }

        for (RaftLogEntry entry : entries) {
            Assert.assertTrue(raftLog.containsIndex(entry.index()));
        }
    }

    @Test
    public void testContainsEntry() throws Exception {
        RaftLogEntry[] entries = generator.generate(max());
        for (RaftLogEntry entry : entries) {
            raftLog.appendEntry(entry);
        }

        for (RaftLogEntry entry : entries) {
            Assert.assertTrue(raftLog.containsEntry(entry.index()));
        }
    }

    @Test
    public void testGetEntry() throws Exception {
        RaftLogEntry[] entries = generator.generate(max());
        for (RaftLogEntry entry : entries) {
            raftLog.appendEntry(entry);
        }

        for (RaftLogEntry entry : entries) {
            ReflectionAssert.assertReflectionEquals(entry, raftLog.getEntry(entry.index()));
        }
    }
}
