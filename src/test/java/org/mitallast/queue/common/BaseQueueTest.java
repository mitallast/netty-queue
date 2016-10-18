package org.mitallast.queue.common;

import org.junit.Before;
import org.mitallast.queue.node.Node;

public abstract class BaseQueueTest extends BaseIntegrationTest {

    private Node node;

    @Before
    public void setUpNode() throws Exception {
        node = createNode();
    }

    public Node node() {
        return node;
    }
}
