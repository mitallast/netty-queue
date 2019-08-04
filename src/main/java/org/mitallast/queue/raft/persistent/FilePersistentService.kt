package org.mitallast.queue.raft.persistent

import com.google.common.base.Preconditions
import com.google.inject.Inject
import io.vavr.collection.Vector
import io.vavr.control.Option
import org.mitallast.queue.common.file.FileService
import org.mitallast.queue.common.logging.LoggingService
import org.mitallast.queue.raft.protocol.LogEntry
import org.mitallast.queue.raft.protocol.RaftSnapshot
import org.mitallast.queue.transport.DiscoveryNode
import java.io.DataOutputStream
import java.io.File
import java.io.IOError
import java.io.IOException

class FilePersistentService @Inject constructor(
    logging: LoggingService,
    private val fileService: FileService
) : PersistentService {
    private val logger = logging.logger()
    private val stateFile: File = fileService.resource("raft", "state.bin")

    private var segment: Long = 0
    private var currentTerm: Long = 0
    private var votedFor: Option<DiscoveryNode> = Option.none()

    init {
        readState()
    }

    private fun readState() {
        if (stateFile.length() == 0L) {
            segment = initialIndex
            currentTerm = 0
            votedFor = Option.none()
            writeState()
            logger.info("initialize state: segment={} term={} voted={}", segment, currentTerm, votedFor)
        } else {
            try {
                fileService.input(stateFile).use { stream ->
                    segment = stream.readLong()
                    currentTerm = stream.readLong()
                    votedFor = if (stream.readBoolean()) {
                        Option.some(DiscoveryNode.codec.read(stream))
                    } else {
                        Option.none()
                    }
                    logger.info("read state: segment={} term={} voted={}", segment, currentTerm, votedFor)
                }
            } catch (e: IOException) {
                throw IOError(e)
            }

        }
    }

    private fun writeState() {
        try {
            fileService.output(stateFile).use { stream ->
                logger.info("write state: segment={} term={} voted={}", segment, currentTerm, votedFor)
                stream.writeLong(segment)
                stream.writeLong(currentTerm)
                if (votedFor.isDefined) {
                    stream.writeBoolean(true)
                    DiscoveryNode.codec.write(stream, votedFor.get())
                } else {
                    stream.writeBoolean(false)
                }
            }
        } catch (e: IOException) {
            throw IOError(e)
        }

    }

    override fun currentTerm(): Long {
        return currentTerm
    }

    override fun votedFor(): Option<DiscoveryNode> {
        return votedFor
    }

    override fun updateState(newTerm: Long, node: Option<DiscoveryNode>) {
        var update = false
        if (currentTerm != newTerm) {
            currentTerm = newTerm
            update = true
        }
        if (votedFor != node) {
            votedFor = node
            update = true
        }
        if (update) {
            writeState()
        }
    }

    private fun updateSegment(segment: Long) {
        if (this.segment != segment) {
            this.segment = segment
            writeState()
        }
    }

    override fun openLog(): ReplicatedLog {
        logger.info("open log: segment={}", segment)
        val segmentFile = segmentFile(segment)
        var entries = Vector.empty<LogEntry>()
        try {
            fileService.input(segmentFile).use { input ->
                while (input.available() > 0) {
                    entries = entries.append(LogEntry.codec.read(input))
                }
                return FileReplicatedLog(
                    segmentFile,
                    fileService.output(segmentFile, true),
                    entries,
                    initialCommittedIndex,
                    segment
                )
            }
        } catch (e: IOException) {
            throw IOError(e)
        }

    }

    private fun segmentFile(segment: Long): File {
        return fileService.resource("raft", segment.toString() + ".log")
    }

    private fun temporaryFile(): File {
        return fileService.temporary("raft", "log.", ".tmp")
    }

    inner class FileReplicatedLog(
        @Volatile private var segmentFile: File,
        @Volatile private var segmentOutput: DataOutputStream,
        @Volatile private var entries: Vector<LogEntry>,
        @Volatile private var committedIndex: Long, @Volatile private var start: Long
    ) : ReplicatedLog {
        @Volatile private var dirty = false

        override val isEmpty: Boolean
            get() = entries.isEmpty

        override fun contains(entry: LogEntry): Boolean {
            return entries.contains(entry)
        }

        override fun entries(): Vector<LogEntry> {
            return entries
        }

        override fun committedEntries(): Int {
            return (committedIndex - start + 1).toInt()
        }

        override fun committedIndex(): Long {
            return committedIndex
        }

        private fun offset(): Long {
            return start - 1
        }

        private fun length(): Long {
            return entries.size() + offset()
        }

        private operator fun get(index: Long): LogEntry {
            return entries.get((index - start).toInt())
        }

        private fun last(): LogEntry {
            return entries.get(entries.size() - 1)
        }

        override fun containsMatchingEntry(otherPrevTerm: Long, otherPrevIndex: Long): Boolean {
            return (otherPrevTerm == 0L && otherPrevIndex == 0L && isEmpty) ||
                (!isEmpty && otherPrevIndex >= committedIndex() &&
                    containsEntryAt(otherPrevIndex) && termAt(otherPrevIndex) == otherPrevTerm)
        }

        override fun lastTerm(): Option<Long> {
            return if (entries.isEmpty) Option.none() else Option.some(last().term)
        }

        override fun lastIndex(): Long {
            return if (entries.isEmpty) 1 else last().index
        }

        override fun prevIndex(): Long {
            return Math.max(0, lastIndex() - 1)
        }

        override fun nextIndex(): Long {
            return if (entries.isEmpty) 1 else last().index + 1
        }

        override fun commit(committedIndex: Long): ReplicatedLog {
            Preconditions.checkArgument(this.committedIndex <= committedIndex, "commit index cannot be less than " + "current commit")
            Preconditions.checkArgument(lastIndex() >= committedIndex, "commit index cannot be greater than last " + "index")
            this.committedIndex = committedIndex
            flush()
            return this
        }

        override fun append(entry: LogEntry): ReplicatedLog {
            Preconditions.checkArgument(entry.index > committedIndex, "entry index should be > committed index")
            Preconditions.checkArgument(entry.index >= start, "entry index should be >= start index")

            if (entry.index <= length()) { // if contains
                val (term) = get(entry.index)
                if (term == entry.term) { // if term matches, entry already contains in log
                    return this
                } else {
                    val prev = entry.index - 1
                    truncate(prev)
                }
            }

            dirty = true
            LogEntry.codec.write(segmentOutput, entry)
            entries = entries.append(entry)
            return this
        }

        override fun append(entries: Vector<LogEntry>): ReplicatedLog {
            for (entry in entries) {
                append(entry)
            }
            return this
        }

        /**
         * truncate index exclusive truncate index
         */
        private fun truncate(truncateIndex: Long) {
            Preconditions.checkArgument(truncateIndex >= committedIndex,
                "truncate index should be > committed index %d", committedIndex)
            Preconditions.checkArgument(truncateIndex < lastIndex(),
                "truncate index should be < last index")

            entries = entries.dropRightUntil { (_, index) -> index <= truncateIndex }

            try {
                segmentOutput.close()

                val tmpSegment = temporaryFile()
                fileService.output(tmpSegment).use { stream ->
                    for (logEntry in entries) {
                        LogEntry.codec.write(stream, logEntry)
                    }
                }

                fileService.move(tmpSegment, segmentFile)

                // recreate file object after move
                this.segmentFile = segmentFile(start)
                this.segmentOutput = fileService.output(this.segmentFile, true)
                dirty = false
            } catch (e: IOException) {
                throw IOError(e)
            }

        }

        override fun entriesBatchFrom(fromIncluding: Long, howMany: Int): Vector<LogEntry> {
            val toSend = slice(fromIncluding, fromIncluding + howMany)
            return if (toSend.isEmpty) {
                toSend
            } else {
                val batchTerm = toSend.get(0).term
                var builder = Vector.empty<LogEntry>()
                toSend
                    .takeWhile { it.term == batchTerm }
                    .forEach { builder = builder.append(it) }
                builder
            }
        }

        override fun slice(from: Long, until: Long): Vector<LogEntry> {
            val fromIndex = (from - start).toInt()
            val toIndex = (until - start + 1).toInt()
            return if (fromIndex >= entries.size()) {
                Vector.empty()
            } else entries.subSequence(Math.max(0, fromIndex), Math.min(toIndex, entries.size()))
        }

        override fun containsEntryAt(index: Long): Boolean {
            return index >= start && index <= length() && get(index).index == index
        }

        override fun termAt(index: Long): Long {
            return if (index <= 0) {
                0
            } else if (!containsEntryAt(index)) {
                throw IllegalArgumentException("Unable to find log entry at index " + index)
            } else {
                get(index).term
            }
        }

        override fun compactWith(snapshot: RaftSnapshot): ReplicatedLog {
            val lastIncludedIndex = snapshot.meta.lastIncludedIndex
            val snapshotEntry = snapshot.toEntry()

            entries = if (entries.isEmpty) {
                Vector.of(snapshotEntry)
            } else {
                if (entries.get(0).index > lastIncludedIndex) {
                    throw IllegalArgumentException("snapshot too old")
                }
                entries.dropUntil { (_, index) -> index > lastIncludedIndex }.prepend(snapshotEntry)
            }

            try {
                if (snapshot.meta.lastIncludedIndex == start) {
                    segmentOutput.close()
                    val tmpSegment = temporaryFile()
                    fileService.output(tmpSegment).use { stream ->
                        for (logEntry in entries) {
                            LogEntry.codec.write(stream, logEntry)
                        }
                    }
                    fileService.move(tmpSegment, segmentFile)

                    // recreate file object after move
                    segmentFile = segmentFile(start)
                    segmentOutput = fileService.output(segmentFile, true)
                    dirty = false
                    return this
                } else {
                    segmentOutput.close()
                    val newSegmentFile = segmentFile(lastIncludedIndex)

                    val newSegmentOutput = fileService.output(newSegmentFile)
                    for (logEntry in entries) {
                        LogEntry.codec.write(newSegmentOutput, logEntry)
                    }
                    newSegmentOutput.flush()
                    updateSegment(snapshot.meta.lastIncludedIndex)

                    fileService.delete(segmentFile)

                    segmentFile = newSegmentFile
                    segmentOutput = newSegmentOutput
                    start = lastIncludedIndex
                    dirty = false
                    return this
                }
            } catch (e: IOException) {
                throw IOError(e)
            }

        }

        override fun hasSnapshot(): Boolean {
            return !entries.isEmpty && entries.get(0).command is RaftSnapshot
        }

        override fun snapshot(): RaftSnapshot {
            return entries.get(0).command as RaftSnapshot
        }

        private fun flush() {
            if (dirty) {
                try {
                    segmentOutput.flush()
                    dirty = false
                } catch (e: IOException) {
                    throw IOError(e)
                }

            }
        }

        override fun close() {
            try {
                segmentOutput.flush()
                dirty = false
                segmentOutput.close()
            } catch (e: IOException) {
                throw IOError(e)
            }
        }

        override fun toString(): String {
            return "ReplicatedLog{" +
                "entries=" + entries +
                ", committedIndex=" + committedIndex +
                ", start=" + start +
                ", file=" + segmentFile.toPath().fileName + " (" + segmentFile.length() + " bytes)" +
                '}'
        }
    }

    companion object {
        private val initialIndex: Long = 1
        private val initialCommittedIndex: Long = 0
    }
}
