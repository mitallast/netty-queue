package org.mitallast.queue.queue.transactional.mmap.meta;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.mmap.MemoryMappedFile;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class QueueMessageMetaSegmentTest extends BaseTest {

    private final Random random = new Random();

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
        final QueueMessageMetaSegment messageMetaSegment = new QueueMessageMetaSegment(mmapFile, total(), 0.7f);
        final List<QueueMessageMeta> metaList = new ArrayList<>(total());
        long start, end;
        start = System.currentTimeMillis();
        for (int i = 0; i < total(); i++) {
            QueueMessageMeta meta = meta();
            metaList.add(meta);
            assert messageMetaSegment.writeMeta(meta);
        }
        end = System.currentTimeMillis();
        printQps("write", total(), start, end);

        start = System.currentTimeMillis();
        for (int i = 0; i < total(); i++) {
            QueueMessageMeta expected = metaList.get(i);
            QueueMessageMeta actual = messageMetaSegment.readMeta(expected.getUuid());
            Assert.assertEquals(expected, actual);
        }
        end = System.currentTimeMillis();
        printQps("read", total(), start, end);
    }

    @Test
    public void testReadWriteConcurrent() throws Exception {
        long start, end;
        final QueueMessageMetaSegment messageMetaSegment = new QueueMessageMetaSegment(mmapFile, total(), 0.7f);
        final List<QueueMessageMeta> metaList = new ArrayList<>(total());
        for (int i = 0; i < total(); i++) {
            QueueMessageMeta meta = meta();
            metaList.add(meta);
        }

        start = System.currentTimeMillis();
        executeConcurrent(new Runnable() {
            @Override
            public void run() {
                try {
                    for (int i = 0; i < total(); i++) {
                        QueueMessageMeta meta = metaList.get(i);
                        assert messageMetaSegment.writeMeta(meta);

                    }
                    for (int i = 0; i < total(); i++) {
                        QueueMessageMeta expected = metaList.get(i);
                        QueueMessageMeta actual = messageMetaSegment.readMeta(expected.getUuid());
                        Assert.assertEquals(expected, actual);
                    }
                } catch (IOException e) {
                    assert false : e;
                }
            }
        });
        end = System.currentTimeMillis();
        printQps("read/write", total() * concurrency() * 2, start, end);
    }

    private QueueMessageMeta meta() {
        return new QueueMessageMeta(
                randomUUID(),
                QueueMessageStatus.QUEUED,
                random.nextInt(),
                random.nextInt(),
                random.nextInt(),
                random.nextInt()
        );
    }
}
