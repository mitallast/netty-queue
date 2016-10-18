package org.mitallast.queue.transport.netty.codec;

import org.mitallast.queue.Version;
import org.mitallast.queue.common.stream.Streamable;

public class MessageTransportFrame implements TransportFrame {

    private final Version version;
    private final Streamable message;

    public MessageTransportFrame(Version version, Streamable message) {
        this.version = version;
        this.message = message;
    }

    @Override
    public Version version() {
        return version;
    }

    @Override
    public TransportFrameType type() {
        return TransportFrameType.MESSAGE;
    }

    @SuppressWarnings("unchecked")
    public <T extends Streamable> T message() {
        return (T) message;
    }

    @Override
    public String toString() {
        return "MessageTransportFrame{" +
                "version=" + version +
                ", message=" + message +
                '}';
    }
}
