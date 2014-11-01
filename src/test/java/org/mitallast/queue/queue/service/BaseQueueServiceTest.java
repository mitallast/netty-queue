package org.mitallast.queue.queue.service;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.Queue;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.QueueMessageUuidDuplicateException;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.*;

public abstract class BaseQueueServiceTest<T extends QueueService> {

    private final static int concurrency = 24;
    private final static int messagesCount = 50000;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    protected T queueService;
    private ExecutorService executorService;

    protected abstract T createQueueService(Settings settings, Settings queueSettings, Queue queue);

    @Before
    public void before() {
        queueService = createQueueService(
                ImmutableSettings.builder()
                        .put("work_dir", folder.getRoot().getPath())
                        .build(),
                ImmutableSettings.builder().build(),
                new Queue("test_queue")
        );
        queueService.start();

        executorService = Executors.newFixedThreadPool(concurrency);
    }

    @After
    public void after() throws InterruptedException {
        queueService.close();
        queueService = null;

        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        executorService = null;
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
                    assert queueMessage.equals(queueService.get(queueMessage.getUuid()));
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
        assert queueService.size() == 0;
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

    protected final void executeConcurrent(RunnableFactory runnableFactory) throws ExecutionException, InterruptedException {
        Future[] futures = new Future[concurrency];
        for (int i = 0; i < concurrency; i++) {
            futures[i] = executorService.submit(runnableFactory.create(i, concurrency));
        }
        for (int i = 0; i < concurrency; i++) {
            futures[i].get();
        }
    }

    protected final void executeConcurrent(Runnable runnable) throws ExecutionException, InterruptedException {
        Future[] futures = new Future[concurrency];
        for (int i = 0; i < concurrency; i++) {
            futures[i] = executorService.submit(runnable);
        }
        for (int i = 0; i < concurrency; i++) {
            futures[i].get();
        }
    }

    protected final QueueMessage[] createMessages() {
        QueueMessage[] messages = new QueueMessage[messagesCount];
        for (int i = 0; i < messagesCount; i++) {
            messages[i] = createMessage();
        }
        return messages;
    }

    protected final QueueMessage[] createMessagesWithUuid() {
        QueueMessage[] messages = new QueueMessage[messagesCount];
        for (int i = 0; i < messagesCount; i++) {
            messages[i] = createMessageWithUuid();
        }
        return messages;
    }

    protected final QueueMessage createMessage() {
        QueueMessage message = new QueueMessage();
        message.setSource("Hello world");
        return message;
    }

    protected final QueueMessage createMessageWithUuid() {
        QueueMessage message = new QueueMessage();
        message.setUuid(UUID.randomUUID());
        message.setSource("Hello world");
        return message;
    }

    public static interface RunnableFactory {
        public Runnable create(int thread, int concurrency);
    }
}
