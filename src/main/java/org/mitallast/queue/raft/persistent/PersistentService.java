package org.mitallast.queue.raft.persistent;

import org.mitallast.queue.raft.Term;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;
import java.util.Optional;

public interface PersistentService {

    Term currentTerm();

    Optional<DiscoveryNode> votedFor();

    void updateState(Term newTerm, Optional<DiscoveryNode> node) throws IOException;

    ReplicatedLog openLog() throws IOException;
}
