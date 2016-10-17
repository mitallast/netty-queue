package org.mitallast.queue.common;

import org.junit.Before;
import org.mitallast.queue.node.Node;
import org.mitallast.queue.transport.TransportClient;
import org.mitallast.queue.transport.TransportService;

public abstract class BaseQueueTest extends BaseIntegrationTest {

    private Node node;
    private String queueName;

    @Before
    public void setUpNode() throws Exception {
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
}
