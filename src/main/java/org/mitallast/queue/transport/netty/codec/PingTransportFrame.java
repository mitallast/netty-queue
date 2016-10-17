package org.mitallast.queue.transport.netty.codec;

import org.mitallast.queue.Version;

public class PingTransportFrame implements TransportFrame {

    public static PingTransportFrame CURRENT = new PingTransportFrame(Version.CURRENT);

    private final Version version;

    public PingTransportFrame(Version version) {
        this.version = version;
    }

    public Version version() {
        return version;
    }

    @Override
    public TransportFrameType type() {
        return TransportFrameType.PING;
    }
}
