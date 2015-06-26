package org.mitallast.queue.transport;

public interface TransportListener {

    void connected(DiscoveryNode node);

    void disconnected(DiscoveryNode node);
}
