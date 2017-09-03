package org.mitallast.queue.crdt.commutative

import com.google.common.base.Preconditions
import gnu.trove.impl.sync.TSynchronizedLongLongMap
import gnu.trove.map.hash.TLongLongHashMap
import gnu.trove.procedure.TLongProcedure
import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.crdt.replication.Replicator

/**
 * Using counter vector allows to implement garbage collection
 */
class GCounter(private val id: Long, private val replica: Long, private val replicator: Replicator) : CmRDT {

    data class SourceAssign(val value: Long) : CmRDT.SourceUpdate {
        companion object {
            val codec = Codec.of(
                ::SourceAssign,
                SourceAssign::value,
                Codec.longCodec()
            )
        }
    }

    data class DownstreamAssign(val replica: Long, val value: Long) : CmRDT.DownstreamUpdate {
        companion object {
            val codec = Codec.of(
                ::DownstreamAssign,
                DownstreamAssign::replica,
                DownstreamAssign::value,
                Codec.longCodec(),
                Codec.longCodec()
            )
        }
    }

    private val counterMap = TSynchronizedLongLongMap(TLongLongHashMap())

    override fun update(event: Message) {
        when (event) {
            is CmRDT.SourceUpdate -> sourceUpdate(event)
            is CmRDT.DownstreamUpdate -> downstreamUpdate(event)
        }
    }

    override fun shouldCompact(event: Message): Boolean {
        return event is DownstreamAssign && event.value < counterMap.get(replica)
    }

    override fun sourceUpdate(update: CmRDT.SourceUpdate) {
        when (update) {
            is SourceAssign -> add(update.value)
        }
    }

    @Synchronized override fun downstreamUpdate(update: CmRDT.DownstreamUpdate) {
        when (update) {
            is DownstreamAssign -> {
                val current = counterMap.get(update.replica)
                if (current < update.value) {
                    counterMap.put(update.replica, update.value)
                }
            }
        }
    }

    fun increment(): Long = add(1)

    fun add(value: Long): Long {
        Preconditions.checkArgument(value >= 0, "must be positive")
        val updated = counterMap.adjustOrPutValue(replica, value, value)
        replicator.append(id, DownstreamAssign(replica, updated))
        return updated
    }

    fun value(): Long {
        val sum = SumProcedure()
        counterMap.forEachValue(sum)
        return sum.value
    }

    private class SumProcedure : TLongProcedure {
        internal var value: Long = 0

        override fun execute(value: Long): Boolean {
            this.value += value
            return true
        }
    }
}
