package org.mitallast.queue.raft.log;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.unit.ByteSizeUnit;

public class SegmentIndexBenchmark extends BaseTest {

    private final static int max = 1000000;

    private SegmentIndex emptySegmentIndex;
    private SegmentIndex fullSegmentIndex;

    @Before
    public void setUp() throws Exception {
        emptySegmentIndex = new SegmentIndex(testFolder.newFile(), (int) ByteSizeUnit.MB.toBytes(1));
        fullSegmentIndex = new SegmentIndex(testFolder.newFile(), (int) ByteSizeUnit.MB.toBytes(1));

        for (int i = 0; i < max; i++) {
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
        for (int i = 0; i < max; i++) {
            emptySegmentIndex.index(i, i * 100, 100);
        }
    }

    @Test
    public void benchContains() throws Exception {
        for (int i = 0; i < max; i++) {
            fullSegmentIndex.contains(i);
        }
    }

    @Test
    public void benchPosition() throws Exception {
        for (int i = 0; i < max; i++) {
            fullSegmentIndex.position(i);
        }
    }

    @Test
    public void benchLength() throws Exception {
        for (int i = 0; i < max; i++) {
            fullSegmentIndex.length(i);
        }
    }
}
