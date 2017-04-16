package org.mitallast.queue.transport;

import org.mitallast.queue.common.stream.Streamable;

public interface TransportChannel {

    void send(Streamable message);

    void close();
}
