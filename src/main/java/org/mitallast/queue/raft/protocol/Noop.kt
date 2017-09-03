package org.mitallast.queue.raft.protocol

import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message

class Noop private constructor() : Message {
    companion object {
        val INSTANCE = Noop()
        val codec = Codec.of(INSTANCE)
    }
}
