package org.mitallast.queue.transport.client;

import org.junit.Test;
import org.mitallast.queue.action.queue.push.PushRequest;
import org.mitallast.queue.action.queue.push.PushResponse;
import org.mitallast.queue.client.base.Client;
import org.mitallast.queue.common.BaseQueueTest;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;
import org.mitallast.queue.queue.QueueMessage;

import java.util.ArrayList;
import java.util.List;

public class TransportClientIntegrationTest extends BaseQueueTest {
    @Test
    public void testPush() throws Exception {
        createQueue();
        assertQueueEmpty();

        Client client = transportClient();

        List<SmartFuture<PushResponse>> futures = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            PushRequest request = new PushRequest(queueName(), new QueueMessage("Hello world"));
            SmartFuture<PushResponse> future = client.queue().pushRequest(request);
            futures.add(future);
        }

        for (SmartFuture<PushResponse> future : futures) {
            PushResponse response = future.get();
            assert response.getUUID() != null;
            assert response.getUUID().getMostSignificantBits() != 0;
            assert response.getUUID().getLeastSignificantBits() != 0;
        }
    }
}
