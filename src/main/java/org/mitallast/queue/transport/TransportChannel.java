package org.mitallast.queue.transport;

import org.mitallast.queue.transport.netty.codec.TransportFrame;

public interface TransportChannel {

    void send(TransportFrame response);

    void close();
}
