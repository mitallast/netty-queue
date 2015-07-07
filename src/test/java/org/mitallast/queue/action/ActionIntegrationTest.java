package org.mitallast.queue.action;

import org.junit.Test;
import org.mitallast.queue.action.queue.push.PushRequest;
import org.mitallast.queue.common.BaseQueueTest;
import org.mitallast.queue.queue.QueueMessage;

public class ActionIntegrationTest extends BaseQueueTest {

    @Test
    public void testSend() throws Exception {
        createQueue();
        assertQueueEmpty();

        long start = System.currentTimeMillis();
        for (int i = 0; i < max(); i++) {
            localClient().send(
                PushRequest.builder()
                    .setQueue(queueName())
                    .setMessage(new QueueMessage("foo"))
                    .build())
                .get();
        }
        long end = System.currentTimeMillis();
        printQps("send", max(), start, end);
    }
}
