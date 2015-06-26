package org.mitallast.queue.transport;

import org.junit.Test;
import org.mitallast.queue.common.BaseIntegrationTest;
import org.mitallast.queue.node.InternalNode;

public class TransportConnectTest extends BaseIntegrationTest {

    @Test
    public void test() throws Exception {
        InternalNode node1 = createNode();
        InternalNode node2 = createNode();

        node1.injector().getInstance(TransportService.class).connectToNode(node2.localNode());
        node2.injector().getInstance(TransportService.class).connectToNode(node1.localNode());

        Thread.sleep(Long.MAX_VALUE);
    }
}
