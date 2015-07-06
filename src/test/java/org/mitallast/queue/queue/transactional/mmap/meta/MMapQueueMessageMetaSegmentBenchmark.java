package org.mitallast.queue.queue.transactional.mmap.meta;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.mmap.MemoryMappedFile;
import org.mitallast.queue.queue.QueueMessageStatus;
import org.mitallast.queue.queue.QueueMessageType;

import java.util.ArrayList;
import java.util.List;

public class MMapQueueMessageMetaSegmentBenchmark extends BaseTest {

    private MemoryMappedFile mmapFile;
    private List<QueueMessageMeta> metaList;
    private MMapQueueMessageMetaSegment messageMetaSegment;

    @Before
    public void setUp() throws Exception {
        mmapFile = new MemoryMappedFile(testFolder.newFile());
        messageMetaSegment = new MMapQueueMessageMetaSegment(mmapFile, total(), 0.7f);
        metaList = new ArrayList<>(total());
        for (int i = 0; i < total(); i++) {
            metaList.add(meta());
        }
    }

    @After
    public void tearDown() throws Exception {
        mmapFile.close();
    }

    @Test
    public void testInsert() throws Exception {
        long start = System.currentTimeMillis();
        for (QueueMessageMeta meta : metaList) {
            assert messageMetaSegment.insert(meta.getUuid()) >= 0;
        }
        long end = System.currentTimeMillis();
        printQps("insert", metaList.size(), start, end);
    }

    @Test
    public void testWriteLock() throws Exception {
        long start = System.currentTimeMillis();
        for (QueueMessageMeta meta : metaList) {
            int pos = messageMetaSegment.insert(meta.getUuid());
            assert pos >= 0;
            assert messageMetaSegment.writeLock(pos);
        }
        long end = System.currentTimeMillis();
        printQps("insert/write", metaList.size(), start, end);
    }

    @Test
    public void testWriteMeta() throws Exception {
        long start = System.currentTimeMillis();
        for (QueueMessageMeta meta : metaList) {
            int pos = messageMetaSegment.insert(meta.getUuid());
            assert pos >= 0;
            assert messageMetaSegment.writeLock(pos);
            assert messageMetaSegment.writeMeta(meta, pos);
        }
        long end = System.currentTimeMillis();
        printQps("insert/lock/meta", metaList.size(), start, end);
    }

    @Test
    public void testReadMeta() throws Exception {
        long start = System.currentTimeMillis();
        for (QueueMessageMeta meta : metaList) {
            int pos = messageMetaSegment.insert(meta.getUuid());
            assert pos >= 0;
            assert messageMetaSegment.writeLock(pos);
            assert messageMetaSegment.writeMeta(meta, pos);
            assert messageMetaSegment.readMeta(pos) != null;
        }
        long end = System.currentTimeMillis();
        printQps("insert/lock/meta/read", metaList.size(), start, end);
    }

    private QueueMessageMeta meta() {
        return new QueueMessageMeta(
            randomUUID(),
            QueueMessageStatus.QUEUED,
            random.nextInt(),
            random.nextInt(),
            QueueMessageType.STRING
        );
    }
}
