package org.mitallast.queue.crdt

import org.mitallast.queue.common.codec.Message

interface Crdt {

    fun update(event: Message)

    fun shouldCompact(event: Message): Boolean
}
