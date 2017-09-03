package org.mitallast.queue.crdt.log

import javaslang.collection.Vector
import org.mitallast.queue.common.codec.Message

import java.io.Closeable

interface ReplicatedLog : Closeable {

    fun index(): Long

    fun append(id: Long, event: Message): LogEntry

    fun entriesFrom(index: Long): Vector<LogEntry>

    fun delete()

    override fun close()
}
