package org.mitallast.queue.raft;

import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.protocol.RaftSnapshot;
import org.mitallast.queue.raft.protocol.RaftSnapshotMetadata;

import java.util.Optional;

public interface ResourceFSM {

    Streamable apply(Streamable message);

    Optional<RaftSnapshot> prepareSnapshot(RaftSnapshotMetadata snapshotMeta);
}
