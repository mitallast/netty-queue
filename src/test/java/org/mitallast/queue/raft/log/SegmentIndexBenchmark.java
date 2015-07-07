package org.mitallast.queue.raft.log;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;

public class SegmentIndexBenchmark extends BaseTest {

    private SegmentIndex emptySegmentIndex;
    private SegmentIndex fullSegmentIndex;

    @Override
    protected int max() {
        return 10000000;
    }

    @Before
    public void setUp() throws Exception {
        emptySegmentIndex = new SegmentIndex(testFolder.newFile(), max());
        fullSegmentIndex = new SegmentIndex(testFolder.newFile(), max());

        for (int i = 0; i < max(); i++) {
            fullSegmentIndex.index(i, i * 100l, 100);
        }
    }

    @After
    public void tearDown() throws Exception {
        if (emptySegmentIndex != null) {
            emptySegmentIndex.close();
            emptySegmentIndex = null;
        }
        if (fullSegmentIndex != null) {
            fullSegmentIndex.close();
            fullSegmentIndex = null;
        }
    }

    @Test
    public void benchIndex() throws Exception {
        long start = System.currentTimeMillis();
        for (int i = 0; i < max(); i++) {
            emptySegmentIndex.index(i, i * 100, 100);
        }
        long end = System.currentTimeMillis();
        printQps("index", max(), start, end);
    }

    @Test
    public void benchContains() throws Exception {
        long start = System.currentTimeMillis();
        for (int i = 0; i < max(); i++) {
            fullSegmentIndex.contains(i);
        }
        long end = System.currentTimeMillis();
        printQps("contains", max(), start, end);
    }

    @Test
    public void benchPosition() throws Exception {
        long start = System.currentTimeMillis();
        for (int i = 0; i < max(); i++) {
            fullSegmentIndex.position(i);
        }
        long end = System.currentTimeMillis();
        printQps("position", max(), start, end);
    }

    @Test
    public void benchLength() throws Exception {
        long start = System.currentTimeMillis();
        for (int i = 0; i < max(); i++) {
            fullSegmentIndex.length(i);
        }
        long end = System.currentTimeMillis();
        printQps("length", max(), start, end);
    }
}
