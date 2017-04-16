package org.mitallast.queue.raft.persistent;

import javaslang.control.Option;
import org.mitallast.queue.transport.DiscoveryNode;

public interface PersistentService {

    long currentTerm();

    Option<DiscoveryNode> votedFor();

    void updateState(long newTerm, Option<DiscoveryNode> node);

    ReplicatedLog openLog();
}
