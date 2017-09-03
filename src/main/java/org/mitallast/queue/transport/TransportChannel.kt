package org.mitallast.queue.transport

import org.mitallast.queue.common.codec.Message

interface TransportChannel {

    fun send(message: Message)

    fun close()
}
