package org.mitallast.queue.transport;

import com.google.common.net.HostAndPort;

public interface TransportService {

    void connectToNode(HostAndPort address);

    void disconnectFromNode(HostAndPort address);

    TransportChannel channel(HostAndPort address);
}
