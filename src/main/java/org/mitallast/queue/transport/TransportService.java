package org.mitallast.queue.transport;

import com.google.common.collect.ImmutableList;

public interface TransportService {

    DiscoveryNode localNode();

    void connectToNode(DiscoveryNode node);

    void disconnectFromNode(DiscoveryNode node);

    ImmutableList<DiscoveryNode> connectedNodes();

    TransportClient client(DiscoveryNode node);

    void addListener(TransportListener listener);

    void removeListener(TransportListener listener);
}
