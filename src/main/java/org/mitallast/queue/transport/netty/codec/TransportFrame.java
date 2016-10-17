package org.mitallast.queue.transport.netty.codec;

import org.mitallast.queue.Version;

public interface TransportFrame {
    Version version();
    TransportFrameType type();
}
