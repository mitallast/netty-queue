package org.mitallast.queue.queue.transactional.mmap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.mmap.MemoryMappedFile;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.transactional.mmap.data.MMapQueueMessageAppendSegment;
import org.mitallast.queue.queue.transactional.mmap.meta.MMapQueueMessageMetaSegment;

import java.io.RandomAccessFile;

public class MMapQueueMessageSegmentTest extends BaseTest {

    private MemoryMappedFile mmapFile1;
    private MemoryMappedFile mmapFile2;

    private MMapQueueMessageSegment segment;

    @Before
    public void setUp() throws Exception {
        int pageSize = 1048576;
        mmapFile1 = new MemoryMappedFile(new RandomAccessFile(testFolder.newFile(), "rw"), pageSize, 50);
        mmapFile2 = new MemoryMappedFile(new RandomAccessFile(testFolder.newFile(), "rw"), pageSize, 50);
        segment = new MMapQueueMessageSegment(
            new MMapQueueMessageAppendSegment(mmapFile1),
            new MMapQueueMessageMetaSegment(mmapFile2, 1024, 0.7f)
        );
    }

    @After
    public void tearDown() throws Exception {
        mmapFile1.close();
        mmapFile2.close();
    }

    @Test
    public void testPush() throws Exception {
        QueueMessage message1 = createMessageWithUuid();
        QueueMessage message2 = createMessageWithUuid();
        QueueMessage message3 = createMessageWithUuid();

        Assert.assertTrue(segment.push(message1));
        Assert.assertTrue(segment.push(message2));
        Assert.assertTrue(segment.push(message3));
    }

    @Test
    public void testPushAndGet() throws Exception {
        QueueMessage message1 = createMessageWithUuid();
        QueueMessage message2 = createMessageWithUuid();
        QueueMessage message3 = createMessageWithUuid();

        segment.push(message1);
        segment.push(message2);
        segment.push(message3);

        Assert.assertEquals(message1, segment.get(message1.getUuid()));
        Assert.assertEquals(message2, segment.get(message2.getUuid()));
        Assert.assertEquals(message3, segment.get(message3.getUuid()));
    }

    @Test
    public void testPushAndLock() throws Exception {
        QueueMessage message1 = createMessageWithUuid();
        QueueMessage message2 = createMessageWithUuid();
        QueueMessage message3 = createMessageWithUuid();

        segment.push(message1);
        segment.push(message2);
        segment.push(message3);

        Assert.assertEquals(message1, segment.lock(message1.getUuid()));
        Assert.assertEquals(message2, segment.lock(message2.getUuid()));
        Assert.assertEquals(message3, segment.lock(message3.getUuid()));

        Assert.assertNull(segment.lock(message1.getUuid()));
        Assert.assertNull(segment.lock(message2.getUuid()));
        Assert.assertNull(segment.lock(message3.getUuid()));
    }

    @Test
    public void testPushAndLockAndPop() throws Exception {
        QueueMessage message1 = createMessageWithUuid();
        QueueMessage message2 = createMessageWithUuid();
        QueueMessage message3 = createMessageWithUuid();

        segment.push(message1);
        segment.push(message2);
        segment.push(message3);

        Assert.assertNotNull(segment.lockAndPop());
        Assert.assertNotNull(segment.lockAndPop());
        Assert.assertNotNull(segment.lockAndPop());

        Assert.assertNull(segment.lockAndPop());
    }

    @Test
    public void testPushAndUnlockAndDelete() throws Exception {
        QueueMessage message1 = createMessageWithUuid();
        QueueMessage message2 = createMessageWithUuid();
        QueueMessage message3 = createMessageWithUuid();

        segment.push(message1);
        segment.push(message2);
        segment.push(message3);

        segment.lock(message1.getUuid());
        segment.lock(message2.getUuid());
        segment.lock(message3.getUuid());

        Assert.assertEquals(message1, segment.unlockAndDelete(message1.getUuid()));
        Assert.assertEquals(message2, segment.unlockAndDelete(message2.getUuid()));
        Assert.assertEquals(message3, segment.unlockAndDelete(message3.getUuid()));
    }

    @Test
    public void testPushAndUnlockAndRollback() throws Exception {
        QueueMessage message1 = createMessageWithUuid();
        QueueMessage message2 = createMessageWithUuid();
        QueueMessage message3 = createMessageWithUuid();

        segment.push(message1);
        segment.push(message2);
        segment.push(message3);

        segment.lock(message1.getUuid());
        segment.lock(message2.getUuid());
        segment.lock(message3.getUuid());

        Assert.assertEquals(message1, segment.unlockAndRollback(message1.getUuid()));
        Assert.assertEquals(message2, segment.unlockAndRollback(message2.getUuid()));
        Assert.assertEquals(message3, segment.unlockAndRollback(message3.getUuid()));
    }
}
