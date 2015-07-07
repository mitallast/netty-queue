package org.mitallast.queue.transport;

import com.google.common.net.HostAndPort;

public interface TransportService {

    HostAndPort localAddress();

    DiscoveryNode localNode();

    void connectToNode(HostAndPort address);

    void disconnectFromNode(HostAndPort address);

    /**
     * @return local client
     */
    TransportClient client();

    TransportClient client(HostAndPort address);

    void addListener(TransportListener listener);

    void removeListener(TransportListener listener);
}
