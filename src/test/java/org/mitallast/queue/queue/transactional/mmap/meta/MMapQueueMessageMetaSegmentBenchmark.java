package org.mitallast.queue.queue.transactional.mmap.meta;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseBenchmark;
import org.mitallast.queue.common.mmap.MemoryMappedFile;
import org.mitallast.queue.queue.QueueMessageStatus;
import org.mitallast.queue.queue.QueueMessageType;

import java.util.ArrayList;
import java.util.List;

public class MMapQueueMessageMetaSegmentBenchmark extends BaseBenchmark {

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
        for (QueueMessageMeta meta : metaList) {
            assert messageMetaSegment.insert(meta.getUuid()) >= 0;
        }
    }

    @Test
    public void testWriteLock() throws Exception {
        for (QueueMessageMeta meta : metaList) {
            int pos = messageMetaSegment.insert(meta.getUuid());
            assert pos >= 0;
            assert messageMetaSegment.writeLock(pos);
        }
    }

    @Test
    public void testWriteMeta() throws Exception {
        for (QueueMessageMeta meta : metaList) {
            int pos = messageMetaSegment.insert(meta.getUuid());
            assert pos >= 0;
            assert messageMetaSegment.writeLock(pos);
            assert messageMetaSegment.writeMeta(meta, pos);
        }
    }

    @Test
    public void testReadMeta() throws Exception {
        for (QueueMessageMeta meta : metaList) {
            int pos = messageMetaSegment.insert(meta.getUuid());
            assert pos >= 0;
            assert messageMetaSegment.writeLock(pos);
            assert messageMetaSegment.writeMeta(meta, pos);
            assert messageMetaSegment.readMeta(pos) != null;
        }
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
