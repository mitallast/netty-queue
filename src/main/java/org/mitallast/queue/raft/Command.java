package org.mitallast.queue.raft;

import org.mitallast.queue.common.stream.Streamable;

public interface Command<T extends Streamable> extends Operation<T> {
}
