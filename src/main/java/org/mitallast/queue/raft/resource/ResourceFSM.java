package org.mitallast.queue.raft.resource;

import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.protocol.RaftSnapshotMetadata;

import java.util.Optional;

public interface ResourceFSM {
    Optional<Streamable> prepareSnapshot(RaftSnapshotMetadata snapshotMeta);
}
