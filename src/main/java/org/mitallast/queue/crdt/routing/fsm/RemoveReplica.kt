package org.mitallast.queue.crdt.routing.fsm

import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message

data class RemoveReplica(val bucket: Int, val replica: Long) : Message {

    companion object {
        val codec = Codec.of(
            ::RemoveReplica,
            RemoveReplica::bucket,
            RemoveReplica::replica,
            Codec.intCodec(),
            Codec.longCodec()
        )
    }
}
