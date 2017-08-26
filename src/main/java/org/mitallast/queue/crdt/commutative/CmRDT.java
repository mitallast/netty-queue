package org.mitallast.queue.crdt.commutative;

import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.crdt.Crdt;

public interface CmRDT extends Crdt {

    interface SourceUpdate extends Message {}

    interface DownstreamUpdate extends Message {}

    void sourceUpdate(SourceUpdate update);

    void downstreamUpdate(DownstreamUpdate update);
}
