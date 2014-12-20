package org.mitallast.queue.queue.transactional.memory;

import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.common.UUIDs;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.queue.Queue;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.transactional.QueueTransaction;
import org.mitallast.queue.queue.transactional.TransactionalQueueService;

public class MemoryTransactionalQueueServiceTest {

    @Test
    public void testLock() throws Exception {
        final TransactionalQueueService queueService = new MemoryTransactionalQueueService(
                ImmutableSettings.EMPTY,
                ImmutableSettings.EMPTY,
                new Queue(UUIDs.generateRandom().toString())
        );

        final QueueMessage queueMessage = new QueueMessage(UUIDs.generateRandom(), UUIDs.generateRandom().toString());
        queueService.push(queueMessage);
        Assert.assertEquals(queueMessage, queueService.lock(queueMessage.getUuid()));
        Assert.assertNull(queueService.lock(queueMessage.getUuid()));
    }

    @Test
    public void testLockAndPop() throws Exception {
        final TransactionalQueueService queueService = new MemoryTransactionalQueueService(
                ImmutableSettings.EMPTY,
                ImmutableSettings.EMPTY,
                new Queue(UUIDs.generateRandom().toString())
        );

        final QueueMessage queueMessage = new QueueMessage(UUIDs.generateRandom(), UUIDs.generateRandom().toString());
        queueService.push(queueMessage);
        Assert.assertEquals(queueMessage, queueService.lockAndPop());
        Assert.assertNull(queueService.lockAndPop());
        Assert.assertNull(queueService.lock(queueMessage.getUuid()));
    }

    @Test
    public void tesUnlockAndDelete() throws Exception {
        final TransactionalQueueService queueService = new MemoryTransactionalQueueService(
                ImmutableSettings.EMPTY,
                ImmutableSettings.EMPTY,
                new Queue(UUIDs.generateRandom().toString())
        );

        final QueueMessage queueMessage = new QueueMessage(UUIDs.generateRandom(), UUIDs.generateRandom().toString());
        queueService.push(queueMessage);
        queueService.lock(queueMessage.getUuid());
        Assert.assertEquals(queueMessage, queueService.unlockAndDelete(queueMessage.getUuid()));
        Assert.assertNull(queueService.lock(queueMessage.getUuid()));
        Assert.assertNull(queueService.lockAndPop());
    }

    @Test
    public void tesUnlockAndRollback() throws Exception {
        final TransactionalQueueService queueService = new MemoryTransactionalQueueService(
                ImmutableSettings.EMPTY,
                ImmutableSettings.EMPTY,
                new Queue(UUIDs.generateRandom().toString())
        );

        final QueueMessage queueMessage = new QueueMessage(UUIDs.generateRandom(), UUIDs.generateRandom().toString());
        queueService.push(queueMessage);
        queueService.lock(queueMessage.getUuid());
        Assert.assertEquals(queueMessage, queueService.unlockAndRollback(queueMessage.getUuid()));
        Assert.assertEquals(queueMessage, queueService.lockAndPop());
        Assert.assertNull(queueService.lock(queueMessage.getUuid()));
        Assert.assertNull(queueService.lockAndPop());
    }

    @Test
    public void testTransactionPushAndCommit() throws Exception {
        final TransactionalQueueService queueService = new MemoryTransactionalQueueService(
                ImmutableSettings.EMPTY,
                ImmutableSettings.EMPTY,
                new Queue(UUIDs.generateRandom().toString())
        );

        final QueueMessage queueMessage = new QueueMessage(UUIDs.generateRandom(), UUIDs.generateRandom().toString());
        final QueueTransaction transaction = queueService.transaction(UUIDs.generateRandom().toString());
        transaction.begin();
        transaction.push(queueMessage);
        transaction.commit();
        Assert.assertEquals(queueMessage, queueService.lock(queueMessage.getUuid()));
        Assert.assertNull(queueService.lockAndPop());
    }

    @Test
    public void testTransactionPushAndRollback() throws Exception {
        final TransactionalQueueService queueService = new MemoryTransactionalQueueService(
                ImmutableSettings.EMPTY,
                ImmutableSettings.EMPTY,
                new Queue(UUIDs.generateRandom().toString())
        );

        final QueueMessage queueMessage = new QueueMessage(UUIDs.generateRandom(), UUIDs.generateRandom().toString());
        final QueueTransaction transaction = queueService.transaction(UUIDs.generateRandom().toString());
        transaction.begin();
        transaction.push(queueMessage);
        transaction.rollback();
        Assert.assertNull(queueService.lock(queueMessage.getUuid()));
        Assert.assertNull(queueService.lockAndPop());
    }

    @Test
    public void testTransactionPopAndCommit() throws Exception {
        final TransactionalQueueService queueService = new MemoryTransactionalQueueService(
                ImmutableSettings.EMPTY,
                ImmutableSettings.EMPTY,
                new Queue(UUIDs.generateRandom().toString())
        );

        final QueueMessage queueMessage = new QueueMessage(UUIDs.generateRandom(), UUIDs.generateRandom().toString());
        final QueueTransaction transaction = queueService.transaction(UUIDs.generateRandom().toString());
        queueService.push(queueMessage);

        transaction.begin();
        Assert.assertEquals(queueMessage, transaction.pop());
        Assert.assertNull(queueService.lock(queueMessage.getUuid()));

        transaction.commit();
        Assert.assertNull(queueService.lock(queueMessage.getUuid()));
        Assert.assertNull(queueService.lockAndPop());
    }

    @Test
    public void testTransactionPopAndRollback() throws Exception {
        final TransactionalQueueService queueService = new MemoryTransactionalQueueService(
                ImmutableSettings.EMPTY,
                ImmutableSettings.EMPTY,
                new Queue(UUIDs.generateRandom().toString())
        );

        final QueueMessage queueMessage = new QueueMessage(UUIDs.generateRandom(), UUIDs.generateRandom().toString());
        final QueueTransaction transaction = queueService.transaction(UUIDs.generateRandom().toString());
        queueService.push(queueMessage);

        transaction.begin();
        Assert.assertEquals(queueMessage, transaction.pop());
        Assert.assertNull(queueService.lock(queueMessage.getUuid()));

        transaction.rollback();
        Assert.assertEquals(queueMessage, queueService.lock(queueMessage.getUuid()));
        Assert.assertNull(queueService.lockAndPop());
    }

    @Test
    public void testTransactionDeleteAndCommit() throws Exception {
        final TransactionalQueueService queueService = new MemoryTransactionalQueueService(
                ImmutableSettings.EMPTY,
                ImmutableSettings.EMPTY,
                new Queue(UUIDs.generateRandom().toString())
        );

        final QueueMessage queueMessage = new QueueMessage(UUIDs.generateRandom(), UUIDs.generateRandom().toString());
        final QueueTransaction transaction = queueService.transaction(UUIDs.generateRandom().toString());
        queueService.push(queueMessage);

        transaction.begin();
        Assert.assertEquals(queueMessage, transaction.delete(queueMessage.getUuid()));
        Assert.assertNull(queueService.lock(queueMessage.getUuid()));

        transaction.commit();
        Assert.assertNull(queueService.lock(queueMessage.getUuid()));
        Assert.assertNull(queueService.lockAndPop());
    }

    @Test
    public void testTransactionDeleteAndRollback() throws Exception {
        final TransactionalQueueService queueService = new MemoryTransactionalQueueService(
                ImmutableSettings.EMPTY,
                ImmutableSettings.EMPTY,
                new Queue(UUIDs.generateRandom().toString())
        );

        final QueueMessage queueMessage = new QueueMessage(UUIDs.generateRandom(), UUIDs.generateRandom().toString());
        final QueueTransaction transaction = queueService.transaction(UUIDs.generateRandom().toString());
        queueService.push(queueMessage);

        transaction.begin();
        Assert.assertEquals(queueMessage, transaction.delete(queueMessage.getUuid()));
        Assert.assertNull(queueService.lock(queueMessage.getUuid()));

        transaction.rollback();
        Assert.assertEquals(queueMessage, queueService.lock(queueMessage.getUuid()));
        Assert.assertNull(queueService.lockAndPop());
    }
}
