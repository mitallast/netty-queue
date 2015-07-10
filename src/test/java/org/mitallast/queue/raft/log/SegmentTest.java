package org.mitallast.queue.raft.log;

import org.junit.After;
import org.junit.Assert;
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
import org.unitils.reflectionassert.ReflectionAssert;

import java.io.File;

public class SegmentTest extends BaseTest {
    private StreamService streamService;
    private SegmentDescriptor descriptor;
    private Segment segment;
    private SegmentIndex segmentIndex;

    private LogEntryGenerator entryGenerator = new LogEntryGenerator(random);

    @Before
    public void setUp() throws Exception {
        streamService = new InternalStreamService(ImmutableSettings.EMPTY);
        new RaftStreamService(streamService);

        descriptor = SegmentDescriptor.builder()
            .setId(0)
            .setIndex(0)
            .setMaxEntries(100)
            .setMaxEntrySize(1000)
            .setMaxSegmentSize(ByteSizeUnit.MB.toBytes(10))
            .setVersion(0).build();

        File file = testFolder.newFile();
        try (StreamOutput output = streamService.output(file)) {
            output.writeStreamable(descriptor.toBuilder());
        }

        segmentIndex = new SegmentIndex(testFolder.newFile(), (int) descriptor.maxEntries());
        segment = new Segment(streamService, file, segmentIndex);
    }

    @After
    public void tearDown() throws Exception {
        segment.close();
        segmentIndex.close();
    }

    @Test
    public void testAppendEntry() throws Exception {
        LogEntry[] entries = entryGenerator.generate((int) segment.descriptor().maxEntries());
        Assert.assertEquals(segment.size(), segmentIndex.nextPosition());
        for (LogEntry entry : entries) {
            segment.appendEntry(entry);
            Assert.assertEquals(segment.size(), segmentIndex.nextPosition());
        }

        for (LogEntry entry : entries) {
            LogEntry actual = segment.getEntry(entry.index());
            Assert.assertNotNull(actual);
            ReflectionAssert.assertReflectionEquals(actual, entry);
        }
    }

    @Test
    public void testReopen() throws Exception {
        LogEntry[] entries = entryGenerator.generate((int) segment.descriptor().maxEntries());
        for (LogEntry entry : entries) {
            segment.appendEntry(entry);
        }

        segment.flush();

        try (SegmentIndex reopenSegmentIndex = new SegmentIndex(segmentIndex.file(), (int) descriptor.maxEntries());
             Segment reopenSegment = new Segment(streamService, segment.file(), reopenSegmentIndex)
        ) {
            ReflectionAssert.assertReflectionEquals(descriptor, reopenSegment.descriptor());
            Assert.assertFalse(reopenSegment.isEmpty());
            Assert.assertTrue(reopenSegment.isFull());
            Assert.assertEquals(segmentIndex.nextPosition(), reopenSegmentIndex.nextPosition());
            Assert.assertEquals(segment.size(), reopenSegment.size());
            Assert.assertEquals(segment.length(), reopenSegment.length());
            Assert.assertEquals(segment.firstIndex(), reopenSegment.firstIndex());
            Assert.assertEquals(segment.lastIndex(), reopenSegment.lastIndex());
            Assert.assertEquals(segment.nextIndex(), reopenSegment.nextIndex());

            for (LogEntry entry : entries) {
                Assert.assertTrue(segment.containsIndex(entry.index()));
                Assert.assertTrue(segment.containsEntry(entry.index()));
                ReflectionAssert.assertReflectionEquals(entry, segment.getEntry(entry.index()));
            }
        }
    }
}
