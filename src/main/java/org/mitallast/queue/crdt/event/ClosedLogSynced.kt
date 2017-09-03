package org.mitallast.queue.crdt.event

data class ClosedLogSynced(val bucket: Int, val replica: Long)
