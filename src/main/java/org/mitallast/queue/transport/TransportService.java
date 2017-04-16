package org.mitallast.queue.transport;

import org.mitallast.queue.common.stream.Streamable;

public interface TransportService {

    void connectToNode(DiscoveryNode node);

    void disconnectFromNode(DiscoveryNode node);

    void send(DiscoveryNode node, Streamable message);
}
