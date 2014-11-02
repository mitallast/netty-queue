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
    public void testEnqueueAndGetConcurrent() throws Exception {
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
    public void testEnqueueAndDeQueueConcurrent() throws Exception {
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
    public void testEnqueuePerformance() throws Exception {
        warmUp();
        QueueMessage[] messages = createMessages();
        long start = System.currentTimeMillis();
        for (QueueMessage message : messages) {
            queueService.enqueue(message);
        }
        long end = System.currentTimeMillis();
        long qps = (long) (messagesCount / (double) (end - start) * 1000.);
        System.out.println("enqueue " + qps + " qps");
    }

    @Test
    public void testEnqueuePerformanceConcurrent() throws Exception {
        warmUp();
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
        long qps = (long) (messagesCount / (double) (end - start) * 1000.);
        System.out.println("enqueue concurrent " + qps + " qps");
    }

    @Test
    public void testEnqueuePerformanceWithUid() throws Exception {
        warmUp();
        QueueMessage[] messages = createMessagesWithUuid();
        long start = System.currentTimeMillis();
        for (QueueMessage message : messages) {
            queueService.enqueue(message);
        }
        long end = System.currentTimeMillis();
        long qps = (long) (messagesCount / (double) (end - start) * 1000.);
        System.out.println("enqueue with uuid " + qps + " qps");
    }

    @Test
    public void testEnqueuePerformanceWithUidConcurrent() throws Exception {
        warmUp();
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
        long qps = (long) (messagesCount / (double) (end - start) * 1000.);
        System.out.println("enqueue with uuid concurrent " + qps + " qps");
    }

    @Test
    public void testDequeuePerformance() throws Exception {
        warmUp();
        final QueueMessage[] messages = createMessages();
        for (QueueMessage message : messages) {
            queueService.enqueue(message);
        }
        long start = System.currentTimeMillis();
        for (int i = 0; i < messagesCount; i++) {
            QueueMessage message = queueService.dequeue();
            assert message != null;
            assert message.getUuid() != null;
        }
        long end = System.currentTimeMillis();
        long qps = (long) (messagesCount / (double) (end - start) * 1000.);
        System.out.println("dequeue " + qps + " qps");
    }

    @Test
    public void testDequeuePerformanceWithUuid() throws Exception {
        warmUp();
        final QueueMessage[] messages = createMessagesWithUuid();
        for (QueueMessage message : messages) {
            queueService.enqueue(message);
        }
        long start = System.currentTimeMillis();
        for (int i = 0; i < messagesCount; i++) {
            QueueMessage message = queueService.dequeue();
            assert message != null;
            assert message.getUuid() != null;
        }
        long end = System.currentTimeMillis();
        long qps = (long) (messagesCount / (double) (end - start) * 1000.);
        System.out.println("dequeue with uuid " + qps + " qps");
    }

    @Test
    public void testDequeuePerformanceConcurrent() throws Exception {
        warmUp();
        final QueueMessage[] messages = createMessages();
        for (QueueMessage message : messages) {
            queueService.enqueue(message);
        }
        long start = System.currentTimeMillis();
        executeConcurrent(new RunnableFactory() {
            @Override
            public Runnable create(final int thread, final int concurrency) {
                return new Runnable() {
                    @Override
                    public void run() {
                        for (int i = thread; i < messagesCount; i += concurrency) {
                            QueueMessage message = queueService.dequeue();
                            assert message != null;
                            assert message.getUuid() != null;
                        }
                    }
                };
            }
        });
        long end = System.currentTimeMillis();
        long qps = (long) (messagesCount / (double) (end - start) * 1000.);
        System.out.println("dequeue concurrent " + qps + " qps");
    }

    @Test
    public void testDequeuePerformanceConcurrentWithUuid() throws Exception {
        warmUp();
        final QueueMessage[] messages = createMessagesWithUuid();
        for (QueueMessage message : messages) {
            queueService.enqueue(message);
        }
        long start = System.currentTimeMillis();
        executeConcurrent(new RunnableFactory() {
            @Override
            public Runnable create(final int thread, final int concurrency) {
                return new Runnable() {
                    @Override
                    public void run() {
                        for (int i = thread; i < messagesCount; i += concurrency) {
                            QueueMessage message = queueService.dequeue();
                            assert message != null;
                            assert message.getUuid() != null;
                        }
                    }
                };
            }
        });
        long end = System.currentTimeMillis();
        long qps = (long) (messagesCount / (double) (end - start) * 1000.);
        System.out.println("dequeue concurrent with uuid " + qps + " qps");
    }

    private void warmUp() throws Exception {
        for (int i = 0; i < messagesCount; i++) {
            queueService.enqueue(createMessageWithUuid());
        }
        for (int i = 0; i < messagesCount; i++) {
            QueueMessage message = queueService.dequeue();
            assert message != null;
            assert message.getUuid() != null;
        }
    }
}
