package org.mitallast.queue.crdt.routing.fsm

import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message

data class CloseReplica(val bucket: Int, val replica: Long) : Message {
    companion object {
        val codec = Codec.of(
            ::CloseReplica,
            CloseReplica::bucket,
            CloseReplica::replica,
            Codec.intCodec(),
            Codec.longCodec()
        )
    }
}
