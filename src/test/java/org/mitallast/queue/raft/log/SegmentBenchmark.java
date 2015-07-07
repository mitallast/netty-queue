package org.mitallast.queue.raft.log;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.stream.InternalStreamService;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.common.unit.ByteSizeUnit;
import org.mitallast.queue.raft.RaftStreamService;
import org.mitallast.queue.raft.log.entry.LogEntry;
import org.mitallast.queue.raft.util.ExecutionContext;

import java.io.File;

public class SegmentBenchmark extends BaseTest {
    private ExecutionContext executionContext;
    private StreamService streamService;
    private SegmentDescriptor descriptor;
    private Segment segment;
    private SegmentIndex segmentIndex;

    private LogEntryGenerator entryGenerator = new LogEntryGenerator(random);

    @Override
    protected int max() {
        return 1000000;
    }

    @Before
    public void setUp() throws Exception {
        streamService = new InternalStreamService(ImmutableSettings.EMPTY);
        new RaftStreamService(streamService);
        executionContext = new ExecutionContext(ImmutableSettings.EMPTY);
        executionContext.start();
        descriptor = SegmentDescriptor.builder()
            .setId(0)
            .setIndex(0)
            .setMaxEntries(max())
            .setMaxEntrySize(1000)
            .setMaxSegmentSize(ByteSizeUnit.GB.toBytes(1))
            .setVersion(0).build();

        File file = testFolder.newFile();
        try (StreamOutput output = streamService.output(file)) {
            output.writeStreamable(descriptor.toBuilder());
        }

        segmentIndex = new SegmentIndex(testFolder.newFile(), (int) descriptor.maxEntries());
        segment = new Segment(streamService, file, segmentIndex, executionContext);
    }

    @After
    public void tearDown() throws Exception {
        executionContext.submit(() -> {
            try {
                segment.close();
                segmentIndex.close();
            } catch (Throwable e) {
                logger.error("error", e);
            }
        }).get();
        executionContext.stop();
        executionContext.close();
    }

    @Test
    public void testAppend() throws Exception {
        executionContext.submit(() -> {
            try {
                LogEntry[] entries = entryGenerator.generate(max());
                long start = System.currentTimeMillis();
                for (LogEntry entry : entries) {
                    segment.appendEntry(entry);
                }
                long end = System.currentTimeMillis();
                printQps("append", max(), start, end);
            } catch (Throwable e) {
                assert false : e;
            }
        }).get();
    }
}
