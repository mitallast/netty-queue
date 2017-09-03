package org.mitallast.queue.raft.persistent

import javaslang.control.Option
import org.mitallast.queue.transport.DiscoveryNode

interface PersistentService {

    fun currentTerm(): Long

    fun votedFor(): Option<DiscoveryNode>

    fun updateState(newTerm: Long, node: Option<DiscoveryNode>)

    fun openLog(): ReplicatedLog
}
