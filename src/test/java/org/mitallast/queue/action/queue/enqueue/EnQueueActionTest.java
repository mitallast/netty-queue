package org.mitallast.queue.action.queue.enqueue;

import org.junit.Test;
import org.mitallast.queue.common.BaseQueueTest;
import org.mitallast.queue.common.UUIDs;
import org.mitallast.queue.queue.QueueMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class EnQueueActionTest extends BaseQueueTest {

    @Test
    public void testSingleThread() throws Exception {
        createQueue();
        int max = 1000000;
        // warm up
        send(max);

        long start = System.currentTimeMillis();
        send(max);
        long end = System.currentTimeMillis();
        System.out.println("total: " + (end - start) + "ms");
        System.out.println((max * 1000 / (end - start)) + " qps");
    }

    @Test
    public void testMultiThread() throws Exception {
        createQueue();
        final int concurrency = 24;
        final int max = 100000;
        // warm up
        send(max);
        final ExecutorService executorService = Executors.newFixedThreadPool(concurrency);
        final List<Future> futures = new ArrayList<>(concurrency);

        long start = System.currentTimeMillis();
        for (int i = 0; i < concurrency; i++) {
            Future future = executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        send(max);
                    } catch (Exception e) {
                        assert false : e;
                    }
                }
            });
            futures.add(future);
        }
        for (Future future : futures) {
            future.get();
        }
        long end = System.currentTimeMillis();
        System.out.println("total: " + (end - start) + "ms");
        System.out.println(((max * concurrency / (end - start)) * 1000) + " qps");
    }

    private void send(int max) throws java.util.concurrent.ExecutionException {
        for (int i = 0; i < max; i++) {
            QueueMessage message = new QueueMessage(
                    UUIDs.generateRandom().toString()
            );
            EnQueueRequest request = new EnQueueRequest(queueName(), message);
            EnQueueResponse response = client().queue().enqueueRequest(request).get();
            assert response.getUUID().equals(message.getUuid());
        }
    }
}
