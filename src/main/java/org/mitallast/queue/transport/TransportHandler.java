package org.mitallast.queue.transport;

import org.mitallast.queue.common.codec.Message;

@FunctionalInterface
public interface TransportHandler<V extends Message> {
    void handle(V message);
}
