package org.mitallast.queue.crdt.commutative

import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.crdt.Crdt

interface CmRDT : Crdt {

    interface SourceUpdate : Message

    interface DownstreamUpdate : Message

    fun sourceUpdate(update: SourceUpdate)

    fun downstreamUpdate(update: DownstreamUpdate)
}
