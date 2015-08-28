package org.mitallast.queue.log;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;

public class SegmentIndexTest extends BaseTest {

    private SegmentIndex segmentIndex;

    @Override
    protected int max() {
        return 10000;
    }

    @Before
    public void setUp() throws Exception {
        segmentIndex = new SegmentIndex(testFolder.newFile(), max());
    }

    @After
    public void tearDown() throws Exception {
        segmentIndex.close();
    }

    @Test
    public void testEmpty() throws Exception {
        assert segmentIndex.size() == 0;
        assert segmentIndex.firstOffset() == -1;
        assert segmentIndex.lastOffset() == -1;
    }

    @Test
    public void testContains() throws Exception {
        LogEntry[] entries = generate();
        for (LogEntry entry : entries) {
            segmentIndex.index(entry.offset, entry.position, entry.length, MessageStatus.QUEUED);
        }
        for (LogEntry entry : entries) {
            Assert.assertTrue(segmentIndex.contains(entry.offset));
        }
    }

    @Test
    public void testSearch() throws Exception {
        LogEntry[] entries = generate();
        for (LogEntry entry : entries) {
            segmentIndex.index(entry.offset, entry.position, entry.length, MessageStatus.QUEUED);
        }
        for (LogEntry entry : entries) {
            Assert.assertEquals(entry.offset * SegmentIndex.ENTRY_SIZE, segmentIndex.search(entry.offset));
        }
    }

    @Test
    public void testPosition() throws Exception {
        LogEntry[] entries = generate();
        for (LogEntry entry : entries) {
            segmentIndex.index(entry.offset, entry.position, entry.length, MessageStatus.QUEUED);
        }
        for (LogEntry entry : entries) {
            Assert.assertEquals(entry.position, segmentIndex.position(entry.offset));
        }
    }

    @Test
    public void testLength() throws Exception {
        LogEntry[] entries = generate();
        for (LogEntry entry : entries) {
            segmentIndex.index(entry.offset, entry.position, entry.length, MessageStatus.QUEUED);
        }
        for (LogEntry entry : entries) {
            Assert.assertEquals(entry.length, segmentIndex.length(entry.offset));
        }
    }

    @Test
    public void testReopen() throws Exception {
        LogEntry[] entries = generate(max() / 2);

        for (LogEntry entry : entries) {
            segmentIndex.index(entry.offset, entry.position, entry.length, MessageStatus.QUEUED);
        }

        segmentIndex.flush();

        try (SegmentIndex reopenSegmentIndex = new SegmentIndex(segmentIndex.file(), max())) {
            Assert.assertEquals(segmentIndex.size(), reopenSegmentIndex.size());
            Assert.assertEquals(segmentIndex.size(), reopenSegmentIndex.size());
            Assert.assertEquals(segmentIndex.firstOffset(), reopenSegmentIndex.firstOffset());
            Assert.assertEquals(segmentIndex.lastOffset(), reopenSegmentIndex.lastOffset());
            Assert.assertEquals(segmentIndex.lastPosition(), reopenSegmentIndex.lastPosition());
            Assert.assertEquals(segmentIndex.lastLength(), reopenSegmentIndex.lastLength());
            Assert.assertEquals(segmentIndex.nextPosition(), reopenSegmentIndex.nextPosition());

            for (LogEntry entry : entries) {
                Assert.assertEquals(entry.toString(), entry.offset * SegmentIndex.ENTRY_SIZE, segmentIndex.search(entry.offset));
                Assert.assertTrue(entry.toString(), reopenSegmentIndex.contains(entry.offset));
                Assert.assertEquals(entry.toString(), entry.position, segmentIndex.position(entry.offset));
                Assert.assertEquals(entry.toString(), entry.length, segmentIndex.length(entry.offset));
            }
        }
    }

    private LogEntry[] generate() {
        return generate(max());
    }

    private LogEntry[] generate(int max) {
        LogEntry[] entries = new LogEntry[max];
        long position = 0;
        for (int i = 0; i < max; i++) {
            int length = random.nextInt(1000) + 1;
            entries[i] = new LogEntry(i, position, length);
            position += length;
            assert position > 0;
        }
        return entries;
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

        @Override
        public String toString() {
            return "LogEntry{" +
                "offset=" + offset +
                ", position=" + position +
                ", length=" + length +
                '}';
        }
    }
}
