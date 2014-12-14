package org.mitallast.queue.queue.service;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.mitallast.queue.common.UUIDs;
import org.mitallast.queue.queue.QueueMessage;

import java.util.concurrent.*;

public class BaseQueueMessageTest {

    protected final static int concurrency = 24;
    protected final static int messagesCount = 10000;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    protected ExecutorService executorService;

    @Before
    public void setUp() throws Exception {
        executorService = Executors.newFixedThreadPool(concurrency);
    }

    @After
    public void tearDown() throws Exception {
        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        executorService = null;
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
        return createMessages(messagesCount);
    }

    protected final QueueMessage[] createMessages(int messagesCount) {
        QueueMessage[] messages = new QueueMessage[messagesCount];
        for (int i = 0; i < messagesCount; i++) {
            messages[i] = createMessage();
        }
        return messages;
    }

    protected final QueueMessage[] createMessagesWithUuid() {
        return createMessagesWithUuid(messagesCount);
    }

    protected final QueueMessage[] createMessagesWithUuid(int messagesCount) {
        QueueMessage[] messages = new QueueMessage[messagesCount];
        for (int i = 0; i < messagesCount; i++) {
            messages[i] = createMessageWithUuid();
        }
        return messages;
    }

    protected final QueueMessage createMessage() {
        QueueMessage message = new QueueMessage();
        message.setSource(UUIDs.generateRandom().toString());
        return message;
    }

    protected final QueueMessage createMessageWithUuid() {
        QueueMessage message = new QueueMessage();
        message.setUuid(UUIDs.generateRandom());
        message.setSource(UUIDs.generateRandom().toString());
        return message;
    }

    public static interface RunnableFactory {
        public Runnable create(int thread, int concurrency);
    }
}
