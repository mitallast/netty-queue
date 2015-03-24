package org.mitallast.queue.queue.transactional.mmap.meta;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.mmap.MemoryMappedFile;
import org.mitallast.queue.queue.QueueMessageType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MMapQueueMessageMetaSegmentTest extends BaseTest {

    private final Random random = new Random();

    private MemoryMappedFile mmapFile;

    @Before
    public void setUp() throws Exception {
        mmapFile = new MemoryMappedFile(testFolder.newFile(), 1048576, 50);
    }

    @After
    public void tearDown() throws Exception {
        mmapFile.close();
    }

    @Test
    public void testReadWrite() throws Exception {
        final MMapQueueMessageMetaSegment messageMetaSegment = new MMapQueueMessageMetaSegment(mmapFile, total(), 0.7f);
        final List<QueueMessageMeta> metaList = new ArrayList<>(total());
        long start, end;
        start = System.currentTimeMillis();
        for (int i = 0; i < total(); i++) {
            QueueMessageMeta meta = meta();
            metaList.add(meta);
            assert messageMetaSegment.insert(meta.getUuid());
            assert messageMetaSegment.writeLock(meta.getUuid());
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
        final MMapQueueMessageMetaSegment messageMetaSegment = new MMapQueueMessageMetaSegment(mmapFile, total(), 0.7f);
        final List<QueueMessageMeta> metaList = new ArrayList<>(total());
        for (int i = 0; i < total(); i++) {
            QueueMessageMeta meta = meta();
            metaList.add(meta);
        }

        start = System.currentTimeMillis();
        executeConcurrent((thread, concurrency) -> () -> {
            try {
                for (int i = thread; i < total(); i += concurrency) {
                    QueueMessageMeta expected = metaList.get(i);
                    Assert.assertTrue("insert i=" + i, messageMetaSegment.insert(expected.getUuid()));
                    Assert.assertTrue("lock   i=" + i, messageMetaSegment.writeLock(expected.getUuid()));
                    Assert.assertTrue("write  i=" + i, messageMetaSegment.writeMeta(expected));
                }
                for (int i = thread; i < total(); i += concurrency) {
                    QueueMessageMeta expected = metaList.get(i);
                    QueueMessageMeta actual = messageMetaSegment.readMeta(expected.getUuid());
                    Assert.assertEquals("i=" + i, expected, actual);
                }
            } catch (IOException e) {
                assert false : e;
            }
        });
        end = System.currentTimeMillis();
        printQps("read/write", total() * concurrency() * 2, start, end);
    }

    @Test
    public void testLock() throws IOException {
        final MMapQueueMessageMetaSegment messageMetaSegment = new MMapQueueMessageMetaSegment(mmapFile, total(), 0.7f);
        QueueMessageMeta meta = meta();

        assert messageMetaSegment.insert(meta.getUuid());
        assert messageMetaSegment.writeLock(meta.getUuid());
        assert messageMetaSegment.writeMeta(meta);
        QueueMessageMeta metaLocked = messageMetaSegment.lock(meta.getUuid());
        Assert.assertEquals(meta, metaLocked);
        Assert.assertEquals(QueueMessageStatus.LOCKED, metaLocked.getStatus());
    }

    @Test
    public void testUnlockAndDelete() throws IOException {
        final MMapQueueMessageMetaSegment messageMetaSegment = new MMapQueueMessageMetaSegment(mmapFile, total(), 0.7f);
        QueueMessageMeta meta = meta();

        assert messageMetaSegment.insert(meta.getUuid());
        assert messageMetaSegment.writeLock(meta.getUuid());
        assert messageMetaSegment.writeMeta(meta);
        assert messageMetaSegment.lock(meta.getUuid()) != null;
        QueueMessageMeta deleted = messageMetaSegment.unlockAndDelete(meta.getUuid());

        Assert.assertEquals(meta, deleted);
        Assert.assertEquals(QueueMessageStatus.DELETED, deleted.getStatus());
    }

    @Test
    public void testUnlockAndQueue() throws IOException {
        final MMapQueueMessageMetaSegment messageMetaSegment = new MMapQueueMessageMetaSegment(mmapFile, total(), 0.7f);
        QueueMessageMeta meta = meta();

        assert messageMetaSegment.insert(meta.getUuid());
        assert messageMetaSegment.writeLock(meta.getUuid());
        assert messageMetaSegment.writeMeta(meta);
        assert messageMetaSegment.lock(meta.getUuid()) != null;
        QueueMessageMeta deleted = messageMetaSegment.unlockAndQueue(meta.getUuid());

        Assert.assertEquals(meta, deleted);
        Assert.assertEquals(QueueMessageStatus.QUEUED, deleted.getStatus());
    }

    @Test
    public void testIsGarbage() throws IOException {
        final MMapQueueMessageMetaSegment messageMetaSegment = new MMapQueueMessageMetaSegment(mmapFile, 3, 0.7f);

        QueueMessageMeta meta1 = meta();
        QueueMessageMeta meta2 = meta();
        QueueMessageMeta meta3 = meta();

        messageMetaSegment.insert(meta1.getUuid());
        messageMetaSegment.insert(meta2.getUuid());
        messageMetaSegment.insert(meta3.getUuid());

        messageMetaSegment.writeLock(meta1.getUuid());
        messageMetaSegment.writeLock(meta2.getUuid());
        messageMetaSegment.writeLock(meta3.getUuid());

        messageMetaSegment.writeMeta(meta1);
        messageMetaSegment.writeMeta(meta2);
        messageMetaSegment.writeMeta(meta3);

        messageMetaSegment.lock(meta1.getUuid());
        messageMetaSegment.lock(meta2.getUuid());
        messageMetaSegment.lock(meta3.getUuid());

        messageMetaSegment.unlockAndDelete(meta1.getUuid());
        assert !messageMetaSegment.isGarbage();
        messageMetaSegment.unlockAndDelete(meta2.getUuid());
        assert !messageMetaSegment.isGarbage();
        messageMetaSegment.unlockAndDelete(meta3.getUuid());
        assert messageMetaSegment.isGarbage();
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
