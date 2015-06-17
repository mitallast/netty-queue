package org.mitallast.queue.transport;

import io.netty.buffer.ByteBuf;
import org.mitallast.queue.transport.netty.codec.TransportFrame;

public interface TransportChannel {

    void send(TransportFrame response);

    ByteBuf ioBuffer();

    ByteBuf heapBuffer();
}
