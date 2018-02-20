package org.mitallast.queue.raft.event

import io.vavr.collection.Set
import org.mitallast.queue.transport.DiscoveryNode

data class MembersChanged(val members: Set<DiscoveryNode>)
