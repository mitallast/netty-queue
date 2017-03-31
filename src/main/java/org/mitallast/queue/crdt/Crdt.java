package org.mitallast.queue.crdt;

import org.mitallast.queue.common.stream.Streamable;

public interface Crdt {
    void update(Streamable event);
}
