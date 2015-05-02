package org.mitallast.queue.transport.client;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.Version;
import org.mitallast.queue.action.queue.push.PushRequest;
import org.mitallast.queue.action.queue.push.PushResponse;
import org.mitallast.queue.cluster.DiscoveryNode;
import org.mitallast.queue.common.BaseQueueTest;
import org.mitallast.queue.transport.TransportService;

public class NodeNettyTransportServiceTest extends BaseQueueTest {
    @Test
    public void testPush() throws Exception {
        createQueue();
        assertQueueEmpty();

        DiscoveryNode discoveryNode = new DiscoveryNode(
            randomUUID(),
            settings().get("transport.host"),
            settings().getAsInt("transport.port", 9000),
            ImmutableMap.of(),
            Version.CURRENT
        );

        TransportService transportService = node().injector().getInstance(TransportService.class);
        transportService.connectToNode(discoveryNode);

        PushResponse pushResponse = transportService.client(discoveryNode).queue()
            .pushRequest(new PushRequest(queueName(), createMessage()))
            .get();

        Assert.assertNotNull(pushResponse.getMessageUUID());
    }
}
