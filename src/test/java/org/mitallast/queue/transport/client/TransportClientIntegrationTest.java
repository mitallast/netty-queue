package org.mitallast.queue.transport.client;

import org.junit.Test;
import org.mitallast.queue.action.queue.enqueue.EnQueueRequest;
import org.mitallast.queue.action.queue.enqueue.EnQueueResponse;
import org.mitallast.queue.client.base.Client;
import org.mitallast.queue.common.BaseQueueTest;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;
import org.mitallast.queue.queue.QueueMessage;

import java.util.ArrayList;
import java.util.List;

public class TransportClientIntegrationTest extends BaseQueueTest {
    @Test
    public void testEnqueue() throws Exception {
        createQueue();
        assertQueueEmpty();

        Client client = transportClient();

        List<SmartFuture<EnQueueResponse>> futures = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            EnQueueRequest request = new EnQueueRequest(queueName(), new QueueMessage("Hello world"));
            SmartFuture<EnQueueResponse> future = client.queue().enqueueRequest(request);
            futures.add(future);
        }

        for (SmartFuture<EnQueueResponse> future : futures) {
            EnQueueResponse response = future.get();
            assert response.getUUID() != null;
            assert response.getUUID().getMostSignificantBits() != 0;
            assert response.getUUID().getLeastSignificantBits() != 0;
        }
    }
}
