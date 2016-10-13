package org.mitallast.queue.transport.netty.codec;

import com.google.common.base.Preconditions;
import org.mitallast.queue.Version;
import org.mitallast.queue.common.stream.Streamable;

public class StreamableTransportFrame extends TransportFrame {
    private final Streamable message;

    protected StreamableTransportFrame(Version version, long request, Streamable message) {
        super(version, request);
        Preconditions.checkNotNull(message);
        this.message = message;
    }

    @Override
    public boolean streamable() {
        return true;
    }

    @SuppressWarnings("unchecked")
    public <T extends Streamable> T message() {
        return (T) message;
    }

    public static StreamableTransportFrame of(Streamable message) {
        return of(0, message);
    }

    public static StreamableTransportFrame of(long request, Streamable message) {
        return of(Version.CURRENT, request, message);
    }

    public static StreamableTransportFrame of(Version version, long request, Streamable message) {
        return new StreamableTransportFrame(version, request, message);
    }
}
