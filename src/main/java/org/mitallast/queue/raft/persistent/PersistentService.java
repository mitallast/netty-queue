package org.mitallast.queue.raft.persistent;

import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;
import java.util.Optional;

public interface PersistentService {

    long currentTerm();

    Optional<DiscoveryNode> votedFor();

    void updateState(long newTerm, Optional<DiscoveryNode> node) throws IOException;

    ReplicatedLog openLog() throws IOException;
}
