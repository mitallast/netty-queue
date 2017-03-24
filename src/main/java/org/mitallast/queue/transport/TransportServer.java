package org.mitallast.queue.transport;
import org.mitallast.queue.proto.raft.DiscoveryNode;

public interface TransportServer {

    int DEFAULT_PORT = 8900;

    DiscoveryNode localNode();
}
