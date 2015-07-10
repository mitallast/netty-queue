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
import org.mitallast.queue.raft.log.entry.LogEntry;
import org.unitils.reflectionassert.ReflectionAssert;

public class LogTest extends BaseTest {
    private SegmentManager segmentManager;
    private Log log;

    private LogEntryGenerator generator = new LogEntryGenerator(random);

    @Before
    public void setUp() throws Exception {
        StreamService streamService = new InternalStreamService(ImmutableSettings.EMPTY);
        new RaftStreamService(streamService);
        SegmentFileService fileService = new SegmentFileService(ImmutableSettings.builder()
            .put("work_dir", testFolder.getRoot())
            .build());
        SegmentDescriptorService descriptorService = new SegmentDescriptorService(ImmutableSettings.EMPTY, fileService, streamService);
        SegmentIndexService indexService = new SegmentIndexService(ImmutableSettings.EMPTY, fileService);
        SegmentService segmentService = new SegmentService(ImmutableSettings.EMPTY, streamService, fileService, indexService);
        segmentManager = new SegmentManager(ImmutableSettings.EMPTY, descriptorService, segmentService);
        segmentManager.start();
        log = new Log(ImmutableSettings.EMPTY, segmentManager);
    }

    @After
    public void tearDown() throws Exception {
        segmentManager.stop();
        segmentManager.close();
    }

    @Test
    public void testAppend() throws Exception {
        LogEntry[] entries = generator.generate(max());
        for (LogEntry entry : entries) {
            log.appendEntry(entry);
        }
    }

    @Test
    public void testContainsIndex() throws Exception {
        LogEntry[] entries = generator.generate(max());
        for (LogEntry entry : entries) {
            log.appendEntry(entry);
        }

        for (LogEntry entry : entries) {
            Assert.assertTrue(log.containsIndex(entry.index()));
        }
    }

    @Test
    public void testContainsEntry() throws Exception {
        LogEntry[] entries = generator.generate(max());
        for (LogEntry entry : entries) {
            log.appendEntry(entry);
        }

        for (LogEntry entry : entries) {
            Assert.assertTrue(log.containsEntry(entry.index()));
        }
    }

    @Test
    public void testGetEntry() throws Exception {
        LogEntry[] entries = generator.generate(max());
        for (LogEntry entry : entries) {
            log.appendEntry(entry);
        }

        for (LogEntry entry : entries) {
            ReflectionAssert.assertReflectionEquals(entry, log.getEntry(entry.index()));
        }
    }
}
