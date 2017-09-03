package org.mitallast.queue.crdt.log

import com.google.inject.Inject
import com.google.inject.assistedinject.Assisted
import com.typesafe.config.Config
import javaslang.collection.Vector
import org.apache.logging.log4j.LogManager
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.common.file.FileService
import java.util.ArrayList
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Predicate

class FileReplicatedLog @Inject constructor(
    config: Config,
    private val fileService: FileService,
    @param:Assisted private val compactionFilter: Predicate<LogEntry>,
    @Assisted index: Int,
    @Assisted replica: Long
) : ReplicatedLog {
    private val logger = LogManager.getLogger()
    private val segmentSize = config.getInt("crdt.segment.size")
    private val serviceName = String.format("crdt/%d/log/%d", index, replica)

    private val compaction = Executors.newSingleThreadExecutor()
    private val segmentsLock = ReentrantLock()
    @Volatile private var segments = Vector.empty<Segment>()
    @Volatile private var lastSegment: Segment

    private val index = AtomicLong(0)

    init {
        val offsets = fileService.resources(serviceName, "regex:event.[0-9]+.log")
            .map { it.fileName.toString() }
            .map { it.substring(6, it.length - 4) }
            .mapToLong { it.toLong() }
            .sorted()
            .toArray()

        for (i in offsets.indices) {
            segments = segments.append(Segment(i.toLong()))
        }
        if (segments.isEmpty) {
            segments = segments.append(Segment(this.index.get()))
        }
        lastSegment = segments.get(segments.size() - 1)
    }

    override fun index(): Long {
        return index.get()
    }

    override fun append(id: Long, event: Message): LogEntry {
        while (true) {
            var append = lastSegment.append(id, event)
            if (append != null) {
                return append
            }
            var startGC = false
            segmentsLock.lock()
            try {
                if (lastSegment.isFull) {
                    lastSegment = Segment(index.get())
                    segments = segments.append(lastSegment)
                    logger.debug("created segment {}", lastSegment.offset)
                    append = lastSegment.append(id, event)
                    if (append != null) {
                        startGC = true
                        return append
                    }
                }
            } finally {
                segmentsLock.unlock()
                if (startGC) {
                    startGC()
                }
            }
        }
    }

    override fun entriesFrom(index: Long): Vector<LogEntry> {
        var builder = Vector.empty<LogEntry>()
        for (segment in segments.reverse()) {
            synchronized(segment.entries) {
                for (logEntry in segment.entries.reversed()) {
                    if (logEntry.index > index) {
                        builder = builder.append(logEntry)
                    } else {
                        return builder.reverse()
                    }
                }
            }
        }
        return builder.reverse()
    }

    override fun close() {
        segmentsLock.lock()
        try {
            compaction.shutdownNow()
            for (segment in segments) {
                synchronized(segment.entries) {
                    segment.close()
                }
            }
        } finally {
            segmentsLock.unlock()
        }
    }

    override fun delete() {
        close()
        fileService.delete(serviceName)
    }

    private fun startGC() {
        compaction.execute {
            logger.debug("start full GC")
            for (segment in segments) {
                if (segment === lastSegment) {
                    continue
                }
                if (segment.isFull) {
                    logger.debug("compact segment {}", segment.offset)
                    segment.compact()
                    if (segment.isGarbage) {
                        logger.debug("remove segment {}", segment.offset)
                        segment.close()
                        fileService.delete(segment.logFile)
                    }
                }
            }
            segmentsLock.lock()
            try {
                segments = segments.filter { segment -> !segment.isGarbage }
            } finally {
                segmentsLock.unlock()
            }
            logger.debug("end full GC")
        }
    }

    private inner class Segment constructor(val offset: Long) {
        val entries = ArrayList<LogEntry>()
        val logFile = fileService.resource(serviceName, "event.$offset.log")
        private val logOutput = fileService.output(logFile, true)
        private val added = AtomicInteger(0)

        init {
            if (logFile.length() > 0) {
                fileService.input(logFile).use { stream ->
                    if (stream.available() > 0) {
                        while (stream.available() > 0) {
                            entries.add(LogEntry.codec.read(stream))
                        }
                    }
                    if (!entries.isEmpty()) {
                        index.set(entries[entries.size - 1].index + 1)
                    }
                    added.set(entries.size)
                }
            }
        }

        fun append(id: Long, event: Message): LogEntry? {
            synchronized(entries) {
                if (isFull) {
                    return null
                }
                val logEntry = LogEntry(index.incrementAndGet(), id, event)
                LogEntry.codec.write(logOutput, logEntry)
                entries.add(logEntry)
                added.incrementAndGet()
                return logEntry
            }
        }

        val isFull: Boolean = added.get() == segmentSize

        val isGarbage: Boolean = isFull && entries.isEmpty()

        fun compact() {
            synchronized(entries) {
                entries.removeIf(compactionFilter)
            }
        }

        fun close() = logOutput.close()
    }
}
