package org.mitallast.queue.action;

import org.junit.Test;
import org.mitallast.queue.action.queue.enqueue.EnQueueRequest;
import org.mitallast.queue.common.BaseQueueTest;
import org.mitallast.queue.queue.QueueMessage;

public class ActionIntegrationTest extends BaseQueueTest {

    @Test
    public void testSend() throws Exception {
        createQueue();
        assertQueueEmpty();

        long start = System.currentTimeMillis();
        for (int i = 0; i < max(); i++) {
            client().queue()
                    .enqueueRequest(new EnQueueRequest(queueName(), new QueueMessage("foo")))
                    .get();
        }
        long end = System.currentTimeMillis();
        printQps("send", max(), start, end);
    }
}
