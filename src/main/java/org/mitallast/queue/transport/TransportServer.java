package org.mitallast.queue.transport;

public interface TransportServer {

    int DEFAULT_PORT = 8900;

    DiscoveryNode localNode();
}
