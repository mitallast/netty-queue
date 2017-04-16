package org.mitallast.queue.transport;

import org.mitallast.queue.common.stream.Streamable;

@FunctionalInterface
public interface TransportHandler<V extends Streamable> {
    void handle(V message);
}
