package org.mitallast.queue.transport;

import com.google.common.net.HostAndPort;

public interface TransportListener {

    void connected(HostAndPort address);

    void connected(DiscoveryNode node);

    void disconnected(HostAndPort address);

    void disconnected(DiscoveryNode node);
}
