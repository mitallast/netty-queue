package org.mitallast.queue.transport;

import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;

public interface TransportChannel {

    void send(Streamable message) throws IOException;

    void close();
}
