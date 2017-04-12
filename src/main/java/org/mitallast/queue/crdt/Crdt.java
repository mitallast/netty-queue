package org.mitallast.queue.crdt;

import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;

public interface Crdt {

    void update(Streamable event) throws IOException;

    boolean shouldCompact(Streamable event);
}
