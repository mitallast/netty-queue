package org.mitallast.queue.raft.log;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;

public class SegmentIndexTest extends BaseTest {

    private final int max = 1048576;
    private SegmentIndex segmentIndex;

    @Before
    public void setUp() throws Exception {
        segmentIndex = new SegmentIndex(testFolder.newFile(), max);
    }

    @Test
    public void testEmpty() throws Exception {
        assert segmentIndex.size() == 0;
        assert segmentIndex.firstOffset() == -1;
        assert segmentIndex.lastOffset() == -1;
    }

    @Test
    public void testIndex() throws Exception {
        LogEntry[] entries = new LogEntry[max];
        long position = 0;
        for (int i = 0; i < max; i++) {
            int length = random.nextInt(1000) + 1;
            entries[i] = new LogEntry(i, position, length);
            position += length;
        }

        for (LogEntry entry : entries) {
            segmentIndex.index(entry.offset, entry.position, entry.length);
        }

        for (LogEntry entry : entries) {
            Assert.assertEquals("offset: " + entry.offset, entry.offset, segmentIndex.offsetAt(entry.offset * SegmentIndex.ENTRY_SIZE));
            Assert.assertEquals("offset: " + entry.offset, entry.position, segmentIndex.positionAt(entry.offset * SegmentIndex.ENTRY_SIZE));
            Assert.assertEquals("offset: " + entry.offset, entry.length, segmentIndex.lengthAt(entry.offset * SegmentIndex.ENTRY_SIZE));
        }

        for (LogEntry entry : entries) {
            Assert.assertEquals("offset: " + entry.offset, entry.offset * SegmentIndex.ENTRY_SIZE, segmentIndex.search(entry.offset));
        }

        for (LogEntry entry : entries) {
            Assert.assertEquals("offset: " + entry.offset, entry.position, segmentIndex.position(entry.offset));
        }

        for (LogEntry entry : entries) {
            Assert.assertEquals("offset: " + entry.offset, entry.length, segmentIndex.length(entry.offset));
        }
    }

    private static class LogEntry {
        private final int offset;
        private final long position;
        private final int length;

        public LogEntry(int offset, long position, int length) {
            this.offset = offset;
            this.position = position;
            this.length = length;
        }
    }
}
