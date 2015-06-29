package org.mitallast.queue.transport;

import com.google.common.net.HostAndPort;

public interface TransportServer {

    HostAndPort localAddress();

    DiscoveryNode localNode();
}
