package org.mitallast.queue.transport;

import io.netty.buffer.ByteBuf;

public interface TransportChannel {

    void send(TransportFrame response);

    ByteBuf ioBuffer();

    ByteBuf heapBuffer();
}
