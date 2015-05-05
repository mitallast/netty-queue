package org.mitallast.queue.transport;

import org.junit.Test;
import org.mitallast.queue.action.cluster.connect.ClusterConnectRequest;
import org.mitallast.queue.action.cluster.connect.ClusterConnectResponse;
import org.mitallast.queue.cluster.DiscoveryNode;
import org.mitallast.queue.common.BaseIntegrationTest;
import org.mitallast.queue.node.InternalNode;
import org.mitallast.queue.transport.netty.ResponseMapper;

import java.util.Collection;

public class TransportConnectTest extends BaseIntegrationTest {

    @Test
    public void test() throws Exception {
        InternalNode node1 = createNode();
        InternalNode node2 = createNode();

        TransportService transportService1 = node1.injector().getInstance(TransportService.class);
        transportService1.connectToNode(node1.localNode());
        ClusterConnectRequest request = new ClusterConnectRequest();
        request.setDiscoveryNode(node2.localNode());
        ClusterConnectResponse response = transportService1.client(node1.localNode())
            .send(request, new ResponseMapper<>(ClusterConnectResponse::new))
            .get();
        assert response.isConnected();
        transportService1.disconnectFromNode(node1.localNode());

        Collection<DiscoveryNode> discoveryNodes1 = transportService1.connectedNodes();
        assert discoveryNodes1.size() == 1;
        discoveryNodes1.contains(node2.localNode());

        TransportService transportService2 = node2.injector().getInstance(TransportService.class);
        Collection<DiscoveryNode> discoveryNodes2 = transportService2.connectedNodes();
        assert discoveryNodes2.size() == 1;
        discoveryNodes2.contains(node1.localNode());
    }
}
