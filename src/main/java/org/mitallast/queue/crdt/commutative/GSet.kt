package org.mitallast.queue.crdt.commutative

import javaslang.collection.LinkedHashSet
import javaslang.collection.Set
import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.crdt.replication.Replicator

class GSet(private val id: Long, private val replicator: Replicator) : CmRDT {

    data class SourceAdd(val value: Message) : CmRDT.SourceUpdate {
        companion object {
            val codec = Codec.of(
                ::SourceAdd,
                SourceAdd::value,
                Codec.anyCodec()
            )
        }
    }

    data class DownstreamAdd(val value: Message) : CmRDT.DownstreamUpdate {
        companion object {
            val codec = Codec.of(
                ::DownstreamAdd,
                DownstreamAdd::value,
                Codec.anyCodec()
            )
        }
    }

    @Volatile private var values: Set<Message> = LinkedHashSet.empty()

    override fun update(event: Message) {
        when (event) {
            is CmRDT.SourceUpdate -> sourceUpdate(event)
            is CmRDT.DownstreamUpdate -> downstreamUpdate(event)
        }
    }

    override fun shouldCompact(event: Message): Boolean {
        return false
    }

    override fun sourceUpdate(update: CmRDT.SourceUpdate) {
        when (update) {
            is SourceAdd -> add(update.value)
        }
    }

    @Synchronized
    override fun downstreamUpdate(update: CmRDT.DownstreamUpdate) {
        when (update) {
            is DownstreamAdd -> if (!values.contains(update.value)) {
                values = values.add(update.value)
            }
        }
    }

    @Synchronized
    fun add(value: Message) {
        if (!values.contains(value)) {
            values = values.add(value)
            replicator.append(id, DownstreamAdd(value))
        }
    }

    fun values(): Set<Message> = values
}
