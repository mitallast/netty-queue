package org.mitallast.queue.transport;

import org.mitallast.queue.common.builder.Entry;

public interface MessageHandler<Message extends Entry> {
    void dispatch(TransportChannel channel, Message message);
}
