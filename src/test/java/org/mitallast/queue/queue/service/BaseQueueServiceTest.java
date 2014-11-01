package org.mitallast.queue.queue.service;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.Queue;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.QueueMessageUuidDuplicateException;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public abstract class BaseQueueServiceTest<T extends QueueService> extends BaseQueueMessageTest {

    protected T queueService;

    protected abstract T createQueueService(Settings settings, Settings queueSettings, Queue queue);

    @Before
    public void setUp() throws Exception {
        queueService = createQueueService(
            ImmutableSettings.builder()
                .put("work_dir", folder.getRoot().getPath())
                .build(),
            ImmutableSettings.builder().build(),
            new Queue("test_queue")
        );
        queueService.start();
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        queueService.close();
        queueService = null;
        super.tearDown();
    }

    @Test
    public void testEmpty() {
        assert null == queueService.peek();
    }

    @Test
    public void testEnqueueAndPeek() throws IOException {
        QueueMessage message = createMessage();
        queueService.enqueue(message);
        QueueMessage peek = queueService.peek();
        assert peek != null;
        assert message.equals(peek);
    }

    @Test
    public void testEnqueueAndDequeue() throws IOException {
        QueueMessage message = createMessage();
        queueService.enqueue(message);
        QueueMessage peek = queueService.dequeue();
        assert peek != null;
        assert message.equals(peek);
    }

    @Test(expected = QueueMessageUuidDuplicateException.class)
    public void testEnqueueDuplicate() throws IOException {
        QueueMessage message = createMessage();
        queueService.enqueue(message);
        queueService.enqueue(message);
    }

    @Test
    public void testGet() throws IOException {
        QueueMessage message = createMessage();
        queueService.enqueue(message);
        QueueMessage saved = queueService.get(message.getUuid());
        assert message.equals(saved);
    }

    @Test
    public void testEnqueueAndGetConcurrent() throws IOException, InterruptedException, ExecutionException {
        executeConcurrent(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < messagesCount; i++) {
                    QueueMessage queueMessage = createMessage();
                    queueService.enqueue(queueMessage);
                    assert queueMessage.getUuid() != null;
                    QueueMessage queueMessageActual = queueService.get(queueMessage.getUuid());
                    assert queueMessage.equals(queueMessageActual)
                        : queueMessage + " != " + queueMessageActual;
                }
            }
        });
        assert queueService.size() == messagesCount * concurrency;
    }

    @Test
    public void testEnqueueAndDeQueueConcurrent() throws IOException, InterruptedException, ExecutionException {
        executeConcurrent(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < messagesCount; i++) {
                    QueueMessage queueMessage = createMessage();
                    queueService.enqueue(queueMessage);
                }
            }
        });
        assert queueService.size() == messagesCount * concurrency;
        executeConcurrent(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < messagesCount; i++) {
                    QueueMessage queueMessage = queueService.dequeue();
                    assert queueMessage != null;
                    assert queueMessage.getUuid() != null;
                }
            }
        });
    }

    @Test
    public void testWritePerformance() throws IOException {
        QueueMessage[] messages = createMessages();
        long start = System.currentTimeMillis();
        for (QueueMessage message : messages) {
            queueService.enqueue(message);
        }
        long end = System.currentTimeMillis();
        System.out.println("Execute at " + (end - start));
    }

    @Test
    public void testWritePerformanceConcurrent() throws IOException, ExecutionException, InterruptedException {
        final QueueMessage[] messages = createMessages();
        long start = System.currentTimeMillis();
        executeConcurrent(new RunnableFactory() {
            @Override
            public Runnable create(final int thread, final int concurrency) {
                return new Runnable() {
                    @Override
                    public void run() {
                        for (int i = thread; i < messagesCount; i += concurrency) {
                            queueService.enqueue(messages[i]);
                        }
                    }
                };
            }
        });
        long end = System.currentTimeMillis();
        System.out.println("Execute concurrent at " + (end - start));
    }

    @Test
    public void testWritePerformanceWithUid() throws IOException {
        QueueMessage[] messages = createMessagesWithUuid();
        long start = System.currentTimeMillis();
        for (QueueMessage message : messages) {
            queueService.enqueue(message);
        }
        long end = System.currentTimeMillis();
        System.out.println("Execute with uuid at " + (end - start));
    }

    @Test
    public void testWritePerformanceWithUidConcurrent() throws IOException, ExecutionException, InterruptedException {
        final QueueMessage[] messages = createMessagesWithUuid();
        long start = System.currentTimeMillis();
        executeConcurrent(new RunnableFactory() {
            @Override
            public Runnable create(final int thread, final int concurrency) {
                return new Runnable() {
                    @Override
                    public void run() {
                        for (int i = thread; i < messagesCount; i += concurrency) {
                            queueService.enqueue(messages[i]);
                        }
                    }
                };
            }
        });
        long end = System.currentTimeMillis();
        System.out.println("Execute with uuid concurrent at " + (end - start));
    }
}
