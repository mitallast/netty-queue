package org.mitallast.queue.transport.netty.codec;

import org.mitallast.queue.Version;
import org.mitallast.queue.common.stream.Streamable;

public class RequestTransportFrame implements TransportFrame {
    private final Version version;
    private final long request;
    private final Streamable message;

    public RequestTransportFrame(Version version, long request, Streamable message) {
        this.version = version;
        this.request = request;
        this.message = message;
    }

    @Override
    public Version version() {
        return version;
    }

    @Override
    public TransportFrameType type() {
        return TransportFrameType.REQUEST;
    }

    public long request() {
        return request;
    }

    @SuppressWarnings("unchecked")
    public <T extends Streamable> T message() {
        return (T) message;
    }
}
