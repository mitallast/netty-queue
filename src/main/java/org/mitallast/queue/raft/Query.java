package org.mitallast.queue.raft;

import org.mitallast.queue.common.stream.Streamable;

public interface Query<T extends Streamable> extends Operation<T> {

    ConsistencyLevel consistency();
}
