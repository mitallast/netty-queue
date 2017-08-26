package org.mitallast.queue.transport;

import org.mitallast.queue.common.codec.Message;

public interface TransportService {

    void connectToNode(DiscoveryNode node);

    void disconnectFromNode(DiscoveryNode node);

    void send(DiscoveryNode node, Message message);
}
