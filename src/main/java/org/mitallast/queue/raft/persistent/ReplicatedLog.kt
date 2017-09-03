package org.mitallast.queue.raft.persistent

import javaslang.collection.Vector
import javaslang.control.Option
import org.mitallast.queue.raft.protocol.LogEntry
import org.mitallast.queue.raft.protocol.RaftSnapshot

import java.io.Closeable

interface ReplicatedLog : Closeable {

    val isEmpty: Boolean

    operator fun contains(entry: LogEntry): Boolean

    fun entries(): Vector<LogEntry>

    fun committedEntries(): Int

    fun committedIndex(): Long

    fun containsMatchingEntry(otherPrevTerm: Long, otherPrevIndex: Long): Boolean

    fun lastTerm(): Option<Long>

    fun lastIndex(): Long

    fun prevIndex(): Long

    fun nextIndex(): Long

    fun commit(committedIndex: Long): ReplicatedLog

    fun append(entry: LogEntry): ReplicatedLog

    fun append(entries: Vector<LogEntry>): ReplicatedLog

    fun compactWith(snapshot: RaftSnapshot): ReplicatedLog

    fun entriesBatchFrom(fromIncluding: Long, howMany: Int): Vector<LogEntry>

    fun slice(from: Long, until: Long): Vector<LogEntry>

    fun containsEntryAt(index: Long): Boolean

    fun termAt(index: Long): Long

    fun hasSnapshot(): Boolean

    fun snapshot(): RaftSnapshot

    override fun close()
}
