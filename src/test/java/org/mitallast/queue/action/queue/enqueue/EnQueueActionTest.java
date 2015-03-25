package org.mitallast.queue.action.queue.enqueue;

import org.junit.Test;
import org.mitallast.queue.common.BaseQueueTest;
import org.mitallast.queue.queue.QueueMessage;

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
        printQps("send", max(), start, end);
    }

    @Test
    public void testMultiThread() throws Exception {
        createQueue();
        // warm up
        send(max());

        long start = System.currentTimeMillis();
        executeConcurrent((Task) () -> send(max()));
        long end = System.currentTimeMillis();
        printQps("send", total(), start, end);
    }

    private void send(int max) throws Exception {
        for (int i = 0; i < max; i++) {
            QueueMessage message = createMessage();
            EnQueueRequest request = new EnQueueRequest(queueName(), message);
            EnQueueResponse response = client().queue().enqueueRequest(request).get();
            assert response.getUUID().equals(message.getUuid());
        }
    }
}
