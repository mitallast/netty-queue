package org.mitallast.queue.queue.transactional.mmap.data;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.mmap.MemoryMappedFile;

import java.util.ArrayList;
import java.util.List;

public class MMapQueueMessageAppendSegmentBenchmark extends BaseTest {
    private MemoryMappedFile mmapFile;
    private QueueMessageAppendSegment segment;
    private List<ByteBuf> bufferList;
    private long[] offsets;
    private int length;

    @Before
    public void setUp() throws Exception {
        mmapFile = new MemoryMappedFile(testFolder.newFile());
        segment = new MMapQueueMessageAppendSegment(mmapFile);
        bufferList = new ArrayList<>(max());
        offsets = new long[max()];
        length = 256;
        for (int i = 0; i < max(); i++) {
            ByteBuf buffer = Unpooled.buffer(length);
            random.nextBytes(buffer.array());
            buffer.writerIndex(buffer.writerIndex() + length);
            bufferList.add(buffer);
        }
    }

    @After
    public void tearDown() throws Exception {
        mmapFile.close();
    }

    @Test
    public void testWrite() throws Exception {
        long start = System.currentTimeMillis();
        for (int i = 0; i < max(); i++) {
            segment.append(bufferList.get(i));
        }
        long end = System.currentTimeMillis();
        printQps("append", max(), start, end);
    }

    @Test
    public void testReadWrite() throws Exception {
        long start = System.currentTimeMillis();
        for (int i = 0; i < max(); i++) {
            offsets[i] = segment.append(bufferList.get(i));
        }
        ByteBuf buffer = Unpooled.buffer(length);
        for (int i = 0; i < max(); i++) {
            buffer.clear();
            segment.read(buffer, offsets[i], length);
        }
        long end = System.currentTimeMillis();
        printQps("read/append", max(), start, end);
    }
}
