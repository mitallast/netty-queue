package org.mitallast.queue.crdt;

import org.mitallast.queue.common.codec.Message;

public interface Crdt {

    void update(Message event);

    boolean shouldCompact(Message event);
}
