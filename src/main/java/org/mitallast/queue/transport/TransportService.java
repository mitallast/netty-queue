package org.mitallast.queue.transport;

public interface TransportService {

    void connectToNode(DiscoveryNode node);

    void disconnectFromNode(DiscoveryNode node);

    TransportChannel channel(DiscoveryNode node);
}
