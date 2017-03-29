package org.mitallast.queue.crdt.commutative;

import org.mitallast.queue.common.stream.Streamable;

public interface CmRDT<T extends CmRDT<T>> {

    interface SourceUpdate extends Streamable {}

    interface DownstreamUpdate extends Streamable {}

    interface Query extends Streamable {}

    interface QueryResponse extends Streamable {}

    void sourceUpdate(SourceUpdate update);

    void downstreamUpdate(DownstreamUpdate update);

    QueryResponse query(Query query);
}
