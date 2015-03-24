package org.mitallast.queue.queue.transactional.mmap.data;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCounted;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.mmap.MemoryMappedFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MMapQueueMessageAppendSegmentTest extends BaseTest {

    private MemoryMappedFile mmapFile;

    @Before
    public void setUp() throws Exception {
        int pageSize = 1048576;
        mmapFile = new MemoryMappedFile(testFolder.newFile(), pageSize, 50);
    }

    @After
    public void tearDown() throws Exception {
        mmapFile.close();
    }

    @Test
    public void testReadWrite() throws Exception {
        QueueMessageAppendSegment segment = new MMapQueueMessageAppendSegment(mmapFile);
        List<ByteBuf> bufferList = new ArrayList<>(max());
        long[] offsets = new long[max()];
        int length = randomUUID().toString().getBytes().length * 5;
        long start, end;
        for (int i = 0; i < max(); i++) {
            bufferList.add(Unpooled.wrappedBuffer(
                randomUUID().toString().getBytes(),
                randomUUID().toString().getBytes(),
                randomUUID().toString().getBytes(),
                randomUUID().toString().getBytes(),
                randomUUID().toString().getBytes()
            ));
        }

        start = System.currentTimeMillis();
        for (int i = 0; i < max(); i++) {
            long offset = segment.append(bufferList.get(i));
            offsets[i] = offset;
        }
        end = System.currentTimeMillis();
        printQps("append", max(), start, end);

        start = System.currentTimeMillis();
        for (int i = 0; i < max(); i++) {
            ByteBuf buffer = Unpooled.buffer();
            segment.read(buffer, offsets[i], length);
            buffer.resetReaderIndex();
            ByteBuf expected = bufferList.get(i);
            expected.resetReaderIndex();

            Assert.assertTrue(ByteBufUtil.equals(expected, buffer));
            buffer.release();
        }
        end = System.currentTimeMillis();
        printQps("read", max(), start, end);
    }

    @Test
    public void testReadWriteConcurrent() throws Exception {
        final QueueMessageAppendSegment segment = new MMapQueueMessageAppendSegment(mmapFile);
        final List<ByteBuf> bufferList = new ArrayList<>(max());
        final long[] offsets = new long[max()];
        final int length = randomUUID().toString().getBytes().length * 5;
        long start, end;
        for (int i = 0; i < max(); i++) {
            bufferList.add(Unpooled.wrappedBuffer(
                randomUUID().toString().getBytes(),
                randomUUID().toString().getBytes(),
                randomUUID().toString().getBytes(),
                randomUUID().toString().getBytes(),
                randomUUID().toString().getBytes()
            ));
        }

        start = System.currentTimeMillis();
        executeConcurrent((thread, concurrency) -> () -> {
            try {
                for (int i = thread; i < max(); i += concurrency) {
                    long offset = segment.append(bufferList.get(i));
                    offsets[i] = offset;
                }
                ByteBuf buffer = Unpooled.buffer();
                for (int i = thread; i < max(); i += concurrency) {
                    buffer.resetWriterIndex();
                    segment.read(buffer, offsets[i], length);
                    buffer.resetReaderIndex();
                    ByteBuf expected = bufferList.get(i);
                    expected.resetReaderIndex();
                    Assert.assertTrue(ByteBufUtil.equals(expected, buffer));
                }
                buffer.release();
            } catch (IOException e) {
                assert false : e;
            }
        });
        bufferList.forEach(ReferenceCounted::release);
        end = System.currentTimeMillis();
        printQps("read/write concurrent", total() * 2, start, end);
    }
}
