package org.mitallast.queue.action;

import org.junit.Test;
import org.mitallast.queue.action.queue.enqueue.EnQueueRequest;
import org.mitallast.queue.action.queues.create.CreateQueueRequest;
import org.mitallast.queue.common.BaseQueueTest;
import org.mitallast.queue.queue.QueueMessage;

public class ActionIntegrationTest extends BaseQueueTest {

    private final static String queue = "foo";

    @Test
    public void testSend() throws Exception {
        client().queues().createQueue(new CreateQueueRequest(queue)).get();

        final int max = 100000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < max; i++) {
            client().queue()
                .enqueueRequest(new EnQueueRequest(queue, new QueueMessage("foo".getBytes())))
                .get();
        }
        long end = System.currentTimeMillis();
        System.out.println("execute " + max + " at " + (end - start) + "ms");
        System.out.println((max * 1000 / (end - start)) + " qps");
    }
}
