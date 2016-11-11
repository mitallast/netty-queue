package org.mitallast.queue.transport;

import org.mitallast.queue.Version;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.netty.codec.MessageTransportFrame;
import org.mitallast.queue.transport.netty.codec.TransportFrame;

public interface TransportChannel {

    void send(TransportFrame response);

    default void message(Streamable message) {
        send(new MessageTransportFrame(Version.CURRENT, message));
    }

    void close();
}
