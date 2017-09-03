package org.mitallast.queue.crdt.commutative

import javaslang.collection.Set
import javaslang.collection.TreeSet
import javaslang.collection.Vector
import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.crdt.commutative.OrderedGSet.DownstreamAdd
import org.mitallast.queue.crdt.commutative.OrderedGSet.SourceAdd
import org.mitallast.queue.crdt.replication.Replicator

/**
 * Like a G-Set, but require unique timestamp per replica.
 * If two replicas contains equal timestamp, entries will be sorted by replica id for stable sorting.
 */
class OrderedGSet(private val id: Long, private val replica: Long, private val replicator: Replicator) : CmRDT {

    data class SourceAdd(val value: Message, val timestamp: Long) : CmRDT.SourceUpdate {
        companion object {
            val codec = Codec.of(
                ::SourceAdd,
                SourceAdd::value,
                SourceAdd::timestamp,
                Codec.anyCodec(),
                Codec.longCodec()
            )
        }
    }

    data class DownstreamAdd(val value: Message, val timestamp: Long, val replica: Long) : CmRDT.DownstreamUpdate {
        companion object {
            val codec = Codec.of(
                ::DownstreamAdd,
                DownstreamAdd::value,
                DownstreamAdd::timestamp,
                DownstreamAdd::replica,
                Codec.anyCodec(),
                Codec.longCodec(),
                Codec.longCodec()
            )
        }
    }

    private data class Entry(val value: Message, val timestamp: Long, val replica: Long)

    @Volatile
    private var values: Set<Entry> = TreeSet.empty { o1, o2 ->
        var c = java.lang.Long.compare(o1.timestamp, o2.timestamp)
        if (c == 0) {
            c = java.lang.Long.compare(o1.replica, o2.replica)
        }
        c
    }

    override fun update(event: Message) {
        when (event) {
            is CmRDT.SourceUpdate -> sourceUpdate(event)
            is CmRDT.DownstreamUpdate -> downstreamUpdate(event)
        }
    }

    override fun shouldCompact(event: Message): Boolean = false

    override fun sourceUpdate(update: CmRDT.SourceUpdate) {
        when (update) {
            is SourceAdd -> add(update.value, update.timestamp)
        }
    }

    @Synchronized
    override fun downstreamUpdate(update: CmRDT.DownstreamUpdate) {
        when (update) {
            is DownstreamAdd -> {
                val entry = Entry(update.value, update.timestamp, update.replica)
                if (!values.contains(entry)) {
                    values = values.add(entry)
                }
            }
        }
    }

    @Synchronized
    fun add(value: Message, timestamp: Long) {
        val entry = Entry(value, timestamp, replica)
        if (!values.contains(entry)) {
            values = values.add(entry)
            replicator.append(id, DownstreamAdd(value, timestamp, replica))
        }
    }

    fun values(): Vector<Message> {
        return values.toVector().map { it.value }
    }
}

