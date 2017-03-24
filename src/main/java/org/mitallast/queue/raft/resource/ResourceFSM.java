package org.mitallast.queue.raft.resource;

import com.google.protobuf.Message;
import org.mitallast.queue.proto.raft.RaftSnapshotMetadata;

import java.util.Optional;

public interface ResourceFSM {
    Optional<Message> prepareSnapshot(RaftSnapshotMetadata snapshotMeta);
}
