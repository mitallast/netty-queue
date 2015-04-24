package org.mitallast.queue.queue.transactional.mmap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.queue.Queue;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.transactional.QueueTransaction;

import java.io.File;

public class MMapTransactionalQueueServiceTransactionTest extends BaseTest {

    private final static int segmentsSize = 256;
    private File folder;
    private MMapTransactionalQueueService service;

    @Before
    public void setUp() throws Exception {
        folder = testFolder.newFolder();
        service = createService();
    }

    private MMapTransactionalQueueService createService() throws Exception {
        MMapTransactionalQueueService service = new MMapTransactionalQueueService(
            ImmutableSettings.builder()
                .put("work_dir", folder)
                .put("segment.max_size", segmentsSize)
                .build(),
            ImmutableSettings.EMPTY,
            new Queue("test")
        );
        service.start();
        return service;
    }

    @After
    public void tearDown() throws Exception {
        if (service != null) {
            service.stop();
            service.close();
            service = null;
        }
    }

    @Test
    public void testPushAndCommit() throws Exception {
        QueueTransaction transaction = service.transaction(randomUUID());

        QueueMessage message = createMessage();
        transaction.push(message);

        Assert.assertNotNull(message.getUuid());
        Assert.assertNull(service.lock(message.getUuid()));

        transaction.commit();

        Assert.assertEquals(message, service.lock(message.getUuid()));
    }

    @Test
    public void testPushAndRollback() throws Exception {
        QueueTransaction transaction = service.transaction(randomUUID());

        QueueMessage message = createMessage();
        transaction.push(message);

        Assert.assertNotNull(message.getUuid());
        Assert.assertNull(service.lock(message.getUuid()));

        transaction.rollback();

        Assert.assertNull(service.lock(message.getUuid()));
    }

    @Test
    public void testPopAndCommit() throws Exception {
        QueueMessage message = createMessage();
        service.push(message);

        QueueTransaction transaction = service.transaction(randomUUID());

        QueueMessage pop = transaction.pop();

        Assert.assertNotNull(pop);
        Assert.assertNull(service.lock(message.getUuid()));

        transaction.commit();

        Assert.assertNull(service.lock(message.getUuid()));
    }

    @Test
    public void testPopAndRollback() throws Exception {
        QueueMessage message = createMessage();
        service.push(message);

        QueueTransaction transaction = service.transaction(randomUUID());

        QueueMessage pop = transaction.pop();

        Assert.assertNotNull(pop);
        Assert.assertNull(service.lock(message.getUuid()));

        transaction.rollback();

        Assert.assertEquals(message, service.lock(message.getUuid()));
    }

    @Test
    public void testDeleteAndCommit() throws Exception {
        QueueMessage message = createMessage();
        service.push(message);

        QueueTransaction transaction = service.transaction(randomUUID());

        transaction.delete(message.getUuid());

        Assert.assertNull(service.lock(message.getUuid()));

        transaction.commit();

        Assert.assertNull(service.lock(message.getUuid()));
    }

    @Test
    public void testDeleteAndRollback() throws Exception {
        QueueMessage message = createMessage();
        service.push(message);

        QueueTransaction transaction = service.transaction(randomUUID());

        transaction.delete(message.getUuid());

        Assert.assertNull(service.lock(message.getUuid()));

        transaction.rollback();

        Assert.assertEquals(message, service.lock(message.getUuid()));
    }
}
