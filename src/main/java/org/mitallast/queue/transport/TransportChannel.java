package org.mitallast.queue.transport;

import org.mitallast.queue.Version;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.netty.codec.MessageTransportFrame;
import org.mitallast.queue.transport.netty.codec.TransportFrame;

import java.io.IOException;

public interface TransportChannel {

    void send(TransportFrame response) throws IOException;

    default void message(Streamable message) throws IOException {
        send(new MessageTransportFrame(Version.CURRENT, message));
    }

    void close();
}
