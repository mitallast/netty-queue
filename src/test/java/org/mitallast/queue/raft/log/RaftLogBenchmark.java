package org.mitallast.queue.raft.log;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.stream.InternalStreamService;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.raft.RaftStreamService;
import org.mitallast.queue.raft.log.entry.LogEntry;

public class RaftLogBenchmark extends BaseTest {
    private SegmentManager segmentManager;
    private RaftLog log;

    private RaftLogEntryGenerator generator = new RaftLogEntryGenerator(random);

    @Override
    protected int max() {
        return 1000000;
    }

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
        log = new RaftLog(ImmutableSettings.EMPTY, segmentManager);
    }

    @After
    public void tearDown() throws Exception {
        segmentManager.stop();
        segmentManager.close();
    }

    @Test
    public void testAppend() throws Exception {
        LogEntry[] entries = generator.generate(max());
        long start = System.currentTimeMillis();
        for (LogEntry entry : entries) {
            log.appendEntry(entry);
        }
        long end = System.currentTimeMillis();
        printQps("append", max(), start, end);
    }
}
