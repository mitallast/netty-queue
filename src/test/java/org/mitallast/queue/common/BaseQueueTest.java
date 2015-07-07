package org.mitallast.queue.common;

import org.junit.Before;
import org.mitallast.queue.action.queue.pop.PopRequest;
import org.mitallast.queue.action.queue.pop.PopResponse;
import org.mitallast.queue.action.queue.stats.QueueStatsRequest;
import org.mitallast.queue.action.queue.stats.QueueStatsResponse;
import org.mitallast.queue.action.queues.create.CreateQueueRequest;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.node.Node;
import org.mitallast.queue.transport.TransportClient;
import org.mitallast.queue.transport.TransportService;

public abstract class BaseQueueTest extends BaseIntegrationTest {

    private Node node;
    private String queueName;

    @Before
    public void setUp() throws Exception {
        queueName = randomUUID().toString();
        node = createNode();
    }

    public String queueName() {
        return queueName;
    }

    public Node node() {
        return node;
    }

    public TransportClient localClient() {
        return node.injector().getInstance(TransportService.class).client();
    }

    public void createQueue() throws Exception {
        localClient().send(
            CreateQueueRequest.builder()
                .setQueue(queueName())
                .setSettings(ImmutableSettings.EMPTY)
                .build())
            .get();
        assertQueueEmpty();
    }

    public PopResponse pop() throws Exception {
        PopRequest request = PopRequest.builder()
            .setQueue(queueName)
            .build();
        return localClient().<PopRequest, PopResponse>send(request).get();
    }

    public void assertQueueEmpty() throws Exception {
        QueueStatsResponse response = localClient().<QueueStatsRequest, QueueStatsResponse>send(
            QueueStatsRequest.builder()
                .setQueue(queueName())
                .build())
            .get();
        assert response.stats().getSize() == 0 : response.stats();
    }
}
