package org.mitallast.queue.crdt.commutative

import io.vavr.control.Option
import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.crdt.replication.Replicator

class LWWRegister(private val id: Long, private val replicator: Replicator) : CmRDT {

    data class SourceAssign(val value: Message, val timestamp: Long) : CmRDT.SourceUpdate {
        companion object {
            val codec = Codec.of(
                ::SourceAssign,
                SourceAssign::value,
                SourceAssign::timestamp,
                Codec.anyCodec(),
                Codec.longCodec()
            )
        }
    }

    data class DownstreamAssign(val value: Message, val timestamp: Long) : CmRDT.DownstreamUpdate {
        companion object {
            val codec = Codec.of(
                ::DownstreamAssign,
                DownstreamAssign::value,
                DownstreamAssign::timestamp,
                Codec.anyCodec(),
                Codec.longCodec()
            )
        }
    }

    @Volatile private var value = Option.none<Message>()
    @Volatile private var timestamp: Long = 0

    override fun update(event: Message) {
        when (event) {
            is CmRDT.SourceUpdate -> sourceUpdate(event)
            is CmRDT.DownstreamUpdate -> downstreamUpdate(event)
        }
    }

    override fun shouldCompact(event: Message): Boolean {
        return event is DownstreamAssign && event.timestamp < timestamp
    }

    override fun sourceUpdate(update: CmRDT.SourceUpdate) {
        when (update) {
            is SourceAssign -> assign(update.value, update.timestamp)
        }
    }

    @Synchronized
    override fun downstreamUpdate(update: CmRDT.DownstreamUpdate) {
        when (update) {
            is DownstreamAssign -> {
                if (update.timestamp > timestamp) {
                    value = Option.some(update.value)
                    timestamp = update.timestamp
                }
            }
        }
    }

    @Synchronized
    fun assign(value: Message, timestamp: Long) {
        if (this.timestamp < timestamp) {
            this.value = Option.some(value)
            this.timestamp = timestamp
            replicator.append(id, DownstreamAssign(value, timestamp))
        }
    }

    fun value(): Option<Message> = value
}
