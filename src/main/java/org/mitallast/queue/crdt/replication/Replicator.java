package org.mitallast.queue.crdt.replication;

import org.mitallast.queue.common.component.LifecycleComponent;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.crdt.protocol.AppendRejected;
import org.mitallast.queue.crdt.protocol.AppendSuccessful;

import java.io.IOException;

public interface Replicator extends LifecycleComponent {
    void append(long id, Streamable event) throws IOException;
    void successful(AppendSuccessful message) throws IOException;
    void rejected(AppendRejected message) throws IOException;
}
