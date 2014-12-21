package org.mitallast.queue.queue.transactional.mmap.data;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.mitallast.queue.common.UUIDs;
import org.mitallast.queue.common.mmap.MemoryMappedFile;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("NumericOverflow")
public class QueueMessageAppendSegmentTest {

    private final static int max = 10000;
    private final static int concurrency = 24;
    private final static int total = max * concurrency;

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();
    private MemoryMappedFile mmapFile;

    @Before
    public void setUp() throws Exception {
        int pageSize = 1048576;
        mmapFile = new MemoryMappedFile(new RandomAccessFile(testFolder.newFile(), "rw"), pageSize, 50);
    }

    @After
    public void tearDown() throws Exception {
        mmapFile.close();
    }

    @Test
    public void testReadWrite() throws Exception {
        QueueMessageAppendSegment segment = new QueueMessageAppendSegment(mmapFile);
        List<ByteBuf> bufferList = new ArrayList<>(max);
        long[] offsets = new long[max];
        int length = UUIDs.generateRandom().toString().getBytes().length * 5;
        long start, end;
        for (int i = 0; i < max; i++) {
            bufferList.add(Unpooled.wrappedBuffer(
                    UUIDs.generateRandom().toString().getBytes(),
                    UUIDs.generateRandom().toString().getBytes(),
                    UUIDs.generateRandom().toString().getBytes(),
                    UUIDs.generateRandom().toString().getBytes(),
                    UUIDs.generateRandom().toString().getBytes()
            ));
        }

        start = System.currentTimeMillis();
        for (int i = 0; i < max; i++) {
            long offset = segment.append(bufferList.get(i));
            offsets[i] = offset;
        }
        end = System.currentTimeMillis();
        System.out.println("append: " + (end - start));
        System.out.println((total * 1000 / (end - start)) + " q/s");

        start = System.currentTimeMillis();
        for (int i = 0; i < max; i++) {
            ByteBuf buffer = Unpooled.buffer();
            segment.read(buffer, offsets[i], length);
            buffer.resetReaderIndex();
            ByteBuf expected = bufferList.get(i);
            expected.resetReaderIndex();

            Assert.assertTrue(ByteBufUtil.equals(expected, buffer));
        }
        end = System.currentTimeMillis();
        System.out.println("read: " + (end - start));
        System.out.println((total * 1000 / (end - start)) + " q/s");
    }

    @Test
    public void testReadWriteConcurrent() throws Exception {
        final QueueMessageAppendSegment segment = new QueueMessageAppendSegment(mmapFile);
        final List<ByteBuf> bufferList = new ArrayList<>(total);
        final long[] offsets = new long[total];
        final int length = UUIDs.generateRandom().toString().getBytes().length * 5;
        long start, end;
        for (int i = 0; i < total; i++) {
            bufferList.add(Unpooled.wrappedBuffer(
                    UUIDs.generateRandom().toString().getBytes(),
                    UUIDs.generateRandom().toString().getBytes(),
                    UUIDs.generateRandom().toString().getBytes(),
                    UUIDs.generateRandom().toString().getBytes(),
                    UUIDs.generateRandom().toString().getBytes()
            ));
        }

        final ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        try {
            List<Future> futures = new ArrayList<>(concurrency);
            start = System.currentTimeMillis();
            for (int i = 0; i < concurrency; i++) {
                Future future = executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            for (int i = 0; i < total; i++) {
                                long offset = segment.append(Unpooled.wrappedBuffer(bufferList.get(i)));
                                offsets[i] = offset;
                            }
                            for (int i = 0; i < total; i++) {
                                ByteBuf buffer = Unpooled.buffer();
                                segment.read(buffer, offsets[i], length);
                                buffer.resetReaderIndex();
                                ByteBuf expected = Unpooled.wrappedBuffer(bufferList.get(i));
                                expected.resetReaderIndex();

                                Assert.assertTrue(ByteBufUtil.equals(expected, buffer));
                            }
                        } catch (IOException e) {
                            assert false : e;
                        }
                    }
                });
                futures.add(future);
            }
            for (Future future : futures) {
                future.get();
            }
            end = System.currentTimeMillis();
            System.out.println("read/write: " + (end - start));
            System.out.println(((total * concurrency * 2 / (end - start)) * 1000) + " q/s");
        } finally {
            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        }
    }
}
