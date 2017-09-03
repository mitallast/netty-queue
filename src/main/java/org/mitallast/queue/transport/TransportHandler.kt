package org.mitallast.queue.transport

import org.mitallast.queue.common.codec.Message

@FunctionalInterface
interface TransportHandler<V : Message> {
    fun handle(message: V)
}
