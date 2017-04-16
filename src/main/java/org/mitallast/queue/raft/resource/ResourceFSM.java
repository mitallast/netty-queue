package org.mitallast.queue.raft.resource;

import javaslang.control.Option;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.protocol.RaftSnapshotMetadata;

public interface ResourceFSM {
    Option<Streamable> prepareSnapshot(RaftSnapshotMetadata snapshotMeta);
}
