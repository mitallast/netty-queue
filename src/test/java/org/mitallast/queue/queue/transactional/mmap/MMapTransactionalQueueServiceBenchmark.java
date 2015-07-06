package org.mitallast.queue.queue.transactional.mmap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.queue.Queue;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.transactional.QueueTransaction;

import java.util.List;
import java.util.UUID;

public class MMapTransactionalQueueServiceBenchmark extends BaseTest {

    private MMapTransactionalQueueService service;
    private List<QueueMessage> messages;
    private UUID transactionID;

    @Before
    public void setUp() throws Exception {
        service = new MMapTransactionalQueueService(
            ImmutableSettings.builder()
                .put("work_dir", testFolder.newFolder())
                .build(),
            ImmutableSettings.EMPTY,
            new Queue("test")
        );
        service.start();
        messages = createMessages();
        transactionID = randomUUID();
    }

    @After
    public void tearDown() throws Exception {
        service.stop();
        service.close();
    }

    @Test
    public void testPush() throws Exception {
        long start = System.currentTimeMillis();
        for (QueueMessage message : messages) {
            service.push(message);
        }
        long end = System.currentTimeMillis();
        printQps("push", messages.size(), start, end);
    }

    @Test
    public void testGet() throws Exception {
        long start = System.currentTimeMillis();
        for (QueueMessage message : messages) {
            service.push(message);
        }
        for (QueueMessage message : messages) {
            service.get(message.getUuid());
        }
        long end = System.currentTimeMillis();
        printQps("push/get", messages.size(), start, end);
    }

    @Test
    public void testPushTransactional() throws Exception {
        long start = System.currentTimeMillis();
        QueueTransaction transaction = service.transaction(transactionID);
        for (QueueMessage message : messages) {
            service.push(message);
        }
        transaction.commit();
        long end = System.currentTimeMillis();
        printQps("push/commit", messages.size(), start, end);
    }
}
