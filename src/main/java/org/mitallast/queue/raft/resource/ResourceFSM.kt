package org.mitallast.queue.raft.resource

import javaslang.control.Option
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.raft.protocol.RaftSnapshotMetadata

interface ResourceFSM {
    fun prepareSnapshot(snapshotMeta: RaftSnapshotMetadata): Option<Message>
}
