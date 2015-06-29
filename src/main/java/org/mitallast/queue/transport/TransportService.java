package org.mitallast.queue.transport;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;

public interface TransportService {

    DiscoveryNode localNode();

    TransportClient connectToNode(HostAndPort address);

    void connectToNode(DiscoveryNode node);

    void disconnectFromNode(DiscoveryNode node);

    ImmutableList<DiscoveryNode> connectedNodes();

    TransportClient client(DiscoveryNode node);

    void addListener(TransportListener listener);

    void removeListener(TransportListener listener);
}
