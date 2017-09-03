package org.mitallast.queue.raft.event

import javaslang.collection.Set
import org.mitallast.queue.transport.DiscoveryNode

data class MembersChanged(val members: Set<DiscoveryNode>)
