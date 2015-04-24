package org.mitallast.queue.queue.transactional.mmap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseBenchmark;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.queue.Queue;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.transactional.QueueTransaction;

import java.util.List;
import java.util.UUID;

public class MMapTransactionalQueueServiceBenchmark extends BaseBenchmark {

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
        for (QueueMessage message : messages) {
            service.push(message);
        }
    }

    @Test
    public void testGet() throws Exception {
        for (QueueMessage message : messages) {
            service.push(message);
        }
        for (QueueMessage message : messages) {
            service.get(message.getUuid());
        }
    }

    @Test
    public void testPushTransactional() throws Exception {
        QueueTransaction transaction = service.transaction(transactionID);
        for (QueueMessage message : messages) {
            service.push(message);
        }
        transaction.commit();
    }
}
