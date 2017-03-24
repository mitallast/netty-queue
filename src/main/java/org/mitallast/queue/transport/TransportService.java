package org.mitallast.queue.transport;

import org.mitallast.queue.proto.raft.DiscoveryNode;

public interface TransportService {

    void connectToNode(DiscoveryNode node);

    void disconnectFromNode(DiscoveryNode node);

    TransportChannel channel(DiscoveryNode node);
}
