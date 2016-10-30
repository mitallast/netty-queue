package org.mitallast.queue.raft;

import org.mitallast.queue.common.stream.Streamable;

public interface Sender {
    void send(Streamable entry);
}
