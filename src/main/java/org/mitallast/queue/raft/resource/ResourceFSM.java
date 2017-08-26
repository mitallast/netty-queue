package org.mitallast.queue.raft.resource;

import javaslang.control.Option;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.raft.protocol.RaftSnapshotMetadata;

public interface ResourceFSM {
    Option<Message> prepareSnapshot(RaftSnapshotMetadata snapshotMeta);
}
