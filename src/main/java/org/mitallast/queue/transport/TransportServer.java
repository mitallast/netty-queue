package org.mitallast.queue.transport;

import org.mitallast.queue.cluster.DiscoveryNode;

public interface TransportServer {
    DiscoveryNode localNode();
}
