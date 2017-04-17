package org.mitallast.queue.crdt.replication;

import org.mitallast.queue.common.component.LifecycleComponent;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.crdt.protocol.AppendRejected;
import org.mitallast.queue.crdt.protocol.AppendSuccessful;

public interface Replicator extends LifecycleComponent {

    void append(long id, Streamable event);

    void successful(AppendSuccessful message);

    void rejected(AppendRejected message);

    void open();

    void closeAndSync();
}
