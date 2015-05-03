package org.mitallast.queue.transport;

import org.mitallast.queue.cluster.DiscoveryNode;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;

import java.util.Collection;

public interface TransportService {

    void connectToNode(DiscoveryNode node);

    void disconnectFromNode(DiscoveryNode node);

    Collection<DiscoveryNode> connectedNodes();

    SmartFuture<TransportFrame> sendRequest(DiscoveryNode node, TransportFrame frame);

    TransportClient client(DiscoveryNode node);
}
