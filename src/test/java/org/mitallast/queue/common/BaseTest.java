package org.mitallast.queue.common;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.mitallast.queue.queue.QueueMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class BaseTest {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    protected int concurrency() {
        return 24;
    }

    protected int max() {
        return 10000;
    }

    protected final int total() {
        return max() * concurrency();
    }

    protected UUID randomUUID() {
        return UUIDs.generateRandom();
    }

    protected void executeConcurrent(Runnable runnable) throws Exception {
        final ExecutorService executorService = Executors.newFixedThreadPool(concurrency());
        try {
            final List<Future> futures = new ArrayList<>(concurrency());
            for (int i = 0; i < concurrency(); i++) {
                Future future = executorService.submit(runnable);
                futures.add(future);
            }
            for (Future future : futures) {
                future.get();
            }
        } finally {
            executorService.shutdown();
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        }
    }

    protected void executeConcurrent(RunnableFactory runnable) throws Exception {
        final ExecutorService executorService = Executors.newFixedThreadPool(concurrency());
        try {
            final List<Future> futures = new ArrayList<>(concurrency());
            for (int i = 0; i < concurrency(); i++) {
                Future future = executorService.submit(runnable.create(i, concurrency()));
                futures.add(future);
            }
            for (Future future : futures) {
                future.get();
            }
        } finally {
            executorService.shutdown();
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        }
    }

    protected void printQps(String metric, long total, long start, long end) {
        long qps = (long) (total / (double) (end - start) * 1000.);
        System.out.println(metric + ": " + total + " at " + (end - start) + "ms");
        System.out.println(metric + ": " + qps + " qps");
    }

    protected QueueMessage[] createMessages() {
        return createMessages(max());
    }

    protected QueueMessage[] createMessages(int messagesCount) {
        QueueMessage[] messages = new QueueMessage[messagesCount];
        for (int i = 0; i < messagesCount; i++) {
            messages[i] = createMessage();
        }
        return messages;
    }

    protected QueueMessage[] createMessagesWithUuid() {
        return createMessagesWithUuid(max());
    }

    protected QueueMessage[] createMessagesWithUuid(int messagesCount) {
        QueueMessage[] messages = new QueueMessage[messagesCount];
        for (int i = 0; i < messagesCount; i++) {
            messages[i] = createMessageWithUuid();
        }
        return messages;
    }

    protected QueueMessage createMessage() {
        QueueMessage message = new QueueMessage();
        message.setSource(randomUUID().toString());
        return message;
    }

    protected QueueMessage createMessageWithUuid() {
        QueueMessage message = new QueueMessage();
        message.setUuid(randomUUID());
        message.setSource(randomUUID().toString());
        return message;
    }

    public static interface RunnableFactory {
        public Runnable create(int thread, int concurrency);
    }
}
