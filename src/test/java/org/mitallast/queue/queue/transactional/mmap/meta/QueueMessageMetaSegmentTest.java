package org.mitallast.queue.queue.transactional.mmap.meta;

import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.mitallast.queue.common.UUIDs;
import org.mitallast.queue.common.mmap.MemoryMappedFile;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class QueueMessageMetaSegmentTest {

    private final static int max = 10000;
    private final static int concurrency = 24;
    private final static int total = max * concurrency;
    private final Random random = new Random();
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
        final QueueMessageMetaSegment messageMetaSegment = new QueueMessageMetaSegment(mmapFile, total, 0.7f);
        final List<QueueMessageMeta> metaList = new ArrayList<>(total);
        long start, end;
        start = System.currentTimeMillis();
        for (int i = 0; i < total; i++) {
            QueueMessageMeta meta = meta();
            metaList.add(meta);
            assert messageMetaSegment.writeMeta(meta);
        }
        end = System.currentTimeMillis();
        System.out.println("write: " + (end - start));
        System.out.println((total * 1000 / (end - start)) + " q/s");

        start = System.currentTimeMillis();
        for (int i = 0; i < total; i++) {
            QueueMessageMeta expected = metaList.get(i);
            QueueMessageMeta actual = messageMetaSegment.readMeta(expected.getUuid());
            Assert.assertEquals(expected, actual);
        }
        end = System.currentTimeMillis();
        System.out.println("read: " + (end - start));
        System.out.println((total * 1000 / (end - start)) + " q/s");
    }

    @Test
    public void testReadWriteConcurrent() throws Exception {
        long start, end;
        final QueueMessageMetaSegment messageMetaSegment = new QueueMessageMetaSegment(mmapFile, total, 0.7f);
        final ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        try {
            final List<QueueMessageMeta> metaList = new ArrayList<>(total);
            for (int i = 0; i < total; i++) {
                QueueMessageMeta meta = meta();
                metaList.add(meta);
            }
            List<Future> futures = new ArrayList<>(concurrency);
            start = System.currentTimeMillis();
            for (int i = 0; i < concurrency; i++) {
                Future future = executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            for (int i = 0; i < total; i++) {
                                QueueMessageMeta meta = metaList.get(i);
                                assert messageMetaSegment.writeMeta(meta);

                            }
                            for (int i = 0; i < total; i++) {
                                QueueMessageMeta expected = metaList.get(i);
                                QueueMessageMeta actual = messageMetaSegment.readMeta(expected.getUuid());
                                Assert.assertEquals(expected, actual);
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

    private QueueMessageMeta meta() {
        return new QueueMessageMeta(
                UUIDs.generateRandom(),
                QueueMessageStatus.QUEUED,
                random.nextInt(),
                random.nextInt(),
                random.nextInt(),
                random.nextInt()
        );
    }
}
