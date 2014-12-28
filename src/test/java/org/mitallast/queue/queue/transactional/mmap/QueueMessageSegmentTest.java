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

public class QueueMessageSegmentTest extends BaseTest {

    private MemoryMappedFile mmapFile1;
    private MemoryMappedFile mmapFile2;

    @Before
    public void setUp() throws Exception {
        int pageSize = 1048576;
        mmapFile1 = new MemoryMappedFile(new RandomAccessFile(testFolder.newFile(), "rw"), pageSize, 50);
        mmapFile2 = new MemoryMappedFile(new RandomAccessFile(testFolder.newFile(), "rw"), pageSize, 50);
    }

    @After
    public void tearDown() throws Exception {
        mmapFile1.close();
        mmapFile2.close();
    }

    @Test
    public void test() throws Exception {
        QueueMessageSegment segment = new QueueMessageSegment(
                new MMapQueueMessageAppendSegment(mmapFile1),
                new MMapQueueMessageMetaSegment(mmapFile2, 1024, 0.7f)
        );

        QueueMessage message1 = createMessageWithUuid();
        QueueMessage message2 = createMessageWithUuid();
        QueueMessage message3 = createMessageWithUuid();

        assert segment.writeMessage(message1);
        assert segment.writeMessage(message2);
        assert segment.writeMessage(message3);

        Assert.assertEquals(message1, segment.readMessage(message1.getUuid()));
        Assert.assertEquals(message2, segment.readMessage(message2.getUuid()));
        Assert.assertEquals(message3, segment.readMessage(message3.getUuid()));
    }
}
