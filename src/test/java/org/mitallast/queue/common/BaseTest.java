package org.mitallast.queue.common;

import com.google.common.collect.ImmutableList;
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

    protected void executeConcurrent(Task task) throws Exception {
        final ExecutorService executorService = Executors.newFixedThreadPool(concurrency());
        try {
            final List<Future> futures = new ArrayList<>(concurrency());
            for (int i = 0; i < concurrency(); i++) {
                Future future = executorService.submit(() -> {
                    try {
                        task.execute();
                    } catch (Exception e) {
                        assert false : e;
                    }
                });
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

    protected void executeConcurrent(ThreadTask task) throws Exception {
        final ExecutorService executorService = Executors.newFixedThreadPool(concurrency());
        try {
            final List<Future> futures = new ArrayList<>(concurrency());
            for (int i = 0; i < concurrency(); i++) {
                final int thread = i;
                Future future = executorService.submit(() -> {
                    try {
                        task.execute(thread, concurrency());
                    } catch (Exception e) {
                        assert false : e;
                    }
                });
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

    protected List<QueueMessage> createMessages() {
        return createMessages(max());
    }

    protected ImmutableList<QueueMessage> createMessages(int messagesCount) {
        ImmutableList.Builder<QueueMessage> builder = ImmutableList.builder();
        for (int i = 0; i < messagesCount; i++) {
            builder.add(createMessage());
        }
        return builder.build();
    }

    protected ImmutableList<QueueMessage> createMessagesWithUuid() {
        return createMessagesWithUuid(max());
    }

    protected ImmutableList<QueueMessage> createMessagesWithUuid(int messagesCount) {
        ImmutableList.Builder<QueueMessage> builder = ImmutableList.builder();
        for (int i = 0; i < messagesCount; i++) {
            builder.add(createMessageWithUuid());
        }
        return builder.build();
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

    public static interface Task {
        public void execute() throws Exception;
    }

    public static interface ThreadTask {
        public void execute(int thread, int concurrency) throws Exception;
    }
}
