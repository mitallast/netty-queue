package org.mitallast.queue.transport.client;

import org.junit.Test;
import org.mitallast.queue.action.queue.enqueue.EnQueueRequest;
import org.mitallast.queue.action.queue.enqueue.EnQueueResponse;
import org.mitallast.queue.common.BaseQueueTest;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;
import org.mitallast.queue.queue.QueueMessage;

public class TransportClientIntegrationTest extends BaseQueueTest {
    @Test
    public void testEnqueue() throws Exception {
        createQueue();
        assertQueueEmpty();

        TransportClient client = transportClient();

        SmartFuture<EnQueueResponse> future = client.queue().enqueueRequest(new EnQueueRequest(queueName(), new QueueMessage("Hello world")));
        client.flush();
        EnQueueResponse response = future.get();

        assert response.getUUID() != null;
        assert response.getUUID().getMostSignificantBits() != 0;
        assert response.getUUID().getLeastSignificantBits() != 0;
    }
}
