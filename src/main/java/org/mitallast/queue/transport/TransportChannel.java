package org.mitallast.queue.transport;

import org.mitallast.queue.common.codec.Message;

public interface TransportChannel {

    void send(Message message);

    void close();
}
