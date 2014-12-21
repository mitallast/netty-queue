package org.mitallast.queue.queue.transactional.memory;

import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.transactional.TransactionalQueueService;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.UUID;

import static org.mockito.Mockito.*;

public class MemoryQueueTransactionTest extends BaseTest {

    @Mock
    private TransactionalQueueService queueService;
    @Captor
    private ArgumentCaptor<QueueMessage> queueMessageArgumentCaptor;

    private MemoryQueueTransaction transaction;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        assert queueService != null;
        assert queueMessageArgumentCaptor != null;
        transaction = new MemoryQueueTransaction(randomUUID().toString(), queueService);
    }

    @Test
    public void testCommitPush() throws Exception {
        QueueMessage queueMessage = createMessageWithUuid();

        transaction.begin();
        transaction.push(queueMessage);
        verify(queueService, never()).push(any(QueueMessage.class));

        transaction.commit();
        verify(queueService, atLeastOnce()).push(queueMessageArgumentCaptor.capture());
        QueueMessage queueMessageCaptured = queueMessageArgumentCaptor.getValue();
        assert queueMessageCaptured == queueMessage;
    }

    @Test
    public void testRollbackPush() throws Exception {
        QueueMessage queueMessage = createMessageWithUuid();

        transaction.begin();
        transaction.push(queueMessage);
        verify(queueService, never()).push(any(QueueMessage.class));

        transaction.rollback();
        verify(queueService, never()).push(any(QueueMessage.class));
    }

    @Test
    public void testCommitPop() throws Exception {
        QueueMessage queueMessage = createMessageWithUuid();
        when(queueService.lockAndPop()).thenReturn(queueMessage);

        transaction.begin();
        QueueMessage pop = transaction.pop();
        assert pop == queueMessage;

        transaction.commit();
        verify(queueService, atLeastOnce()).unlockAndDelete(any(UUID.class));
    }

    @Test
    public void testRollbackPop() throws Exception {
        QueueMessage queueMessage = createMessageWithUuid();
        when(queueService.lockAndPop()).thenReturn(queueMessage);

        transaction.begin();
        QueueMessage pop = transaction.pop();
        assert pop == queueMessage;

        transaction.rollback();
        verify(queueService, never()).unlockAndDelete(any(UUID.class));
        verify(queueService, atLeastOnce()).unlockAndRollback(any(UUID.class));
    }

    @Test
    public void testCommitDelete() throws Exception {
        QueueMessage queueMessage = createMessageWithUuid();
        when(queueService.lock(queueMessage.getUuid())).thenReturn(queueMessage);


        transaction.begin();
        QueueMessage queueMessageDeleted = transaction.delete(queueMessage.getUuid());
        verify(queueService, atLeastOnce()).lock(any(UUID.class));
        assert queueMessageDeleted == queueMessage;

        transaction.commit();
        verify(queueService, atLeastOnce()).unlockAndDelete(any(UUID.class));
    }

    @Test
    public void testRollbackDelete() throws Exception {
        QueueMessage queueMessage = createMessageWithUuid();
        when(queueService.lock(queueMessage.getUuid())).thenReturn(queueMessage);


        transaction.begin();
        QueueMessage queueMessageDeleted = transaction.delete(queueMessage.getUuid());
        verify(queueService, atLeastOnce()).lock(any(UUID.class));
        assert queueMessageDeleted == queueMessage;

        transaction.rollback();
        verify(queueService, never()).unlockAndDelete(any(UUID.class));
        verify(queueService, atLeastOnce()).unlockAndRollback(any(UUID.class));
    }
}
