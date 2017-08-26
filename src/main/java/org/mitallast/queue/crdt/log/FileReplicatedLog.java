package org.mitallast.queue.crdt.log;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.typesafe.config.Config;
import javaslang.collection.Vector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.common.file.FileService;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

public class FileReplicatedLog implements ReplicatedLog {

    private final static Logger logger = LogManager.getLogger();

    private final int segmentSize;

    private final FileService fileService;
    private final Predicate<LogEntry> compactionFilter;
    private final String serviceName;

    private final ExecutorService compaction = Executors.newSingleThreadExecutor();
    private final ReentrantLock segmentsLock = new ReentrantLock();
    private volatile Vector<Segment> segments = Vector.empty();
    private volatile Segment lastSegment;

    private final AtomicLong index = new AtomicLong(0);

    @Inject
    public FileReplicatedLog(
        Config config,
        FileService fileService,
        @Assisted Predicate<LogEntry> compactionFilter,
        @Assisted int index,
        @Assisted long replica
    ) {
        this.segmentSize = config.getInt("crdt.segment.size");

        this.fileService = fileService;
        this.compactionFilter = compactionFilter;
        this.serviceName = String.format("crdt/%d/log/%d", index, replica);

        long[] offsets = fileService.resources(serviceName, "regex:event.[0-9]+.log")
            .map(path -> path.getFileName().toString())
            .map(name -> name.substring(6, name.length() - 4))
            .mapToLong(Long::parseLong)
            .sorted()
            .toArray();

        for (int i = 0; i < offsets.length; i++) {
            segments = segments.append(new Segment(i));
        }
        if (segments.isEmpty()) {
            segments = segments.append(new Segment(this.index.get()));
        }
        lastSegment = segments.get(segments.size() - 1);
    }

    @Override
    public long index() {
        return index.get();
    }

    @Override
    public LogEntry append(long id, Message event) {
        while (true) {
            LogEntry append = lastSegment.append(id, event);
            if (append != null) {
                return append;
            }
            boolean startGC = false;
            segmentsLock.lock();
            try {
                if (lastSegment.isFull()) {
                    lastSegment = new Segment(index.get());
                    segments = segments.append(lastSegment);
                    logger.debug("created segment {}", lastSegment.offset);
                    append = lastSegment.append(id, event);
                    if (append != null) {
                        startGC = true;
                        return append;
                    }
                }
            } finally {
                segmentsLock.unlock();
                if (startGC) {
                    startGC();
                }
            }
        }
    }

    @Override
    public Vector<LogEntry> entriesFrom(long index) {
        Vector<LogEntry> builder = Vector.empty();
        for (Segment segment : segments.reverse()) {
            synchronized (segment.entries) {
                for (int i = segment.entries.size() - 1; i >= 0; i--) {
                    LogEntry logEntry = segment.entries.get(i);
                    if (logEntry.index() > index) {
                        builder = builder.append(logEntry);
                    } else {
                        return builder.reverse();
                    }
                }
            }
        }
        return builder.reverse();
    }

    @Override
    public void close() {
        segmentsLock.lock();
        try {
            compaction.shutdownNow();
            for (Segment segment : segments) {
                synchronized (segment.entries) {
                    segment.close();
                }
            }
        } finally {
            segmentsLock.unlock();
        }
    }

    @Override
    public void delete() {
        close();
        fileService.delete(serviceName);
    }

    private void startGC() {
        compaction.execute(() -> {
            logger.debug("start full GC");
            for (Segment segment : segments) {
                if (segment == lastSegment) {
                    continue;
                }
                if (segment.isFull()) {
                    logger.debug("compact segment {}", segment.offset);
                    segment.compact();
                    if (segment.isGarbage()) {
                        logger.debug("remove segment {}", segment.offset);
                        segment.close();
                        fileService.delete(segment.logFile);
                    }
                }
            }
            segmentsLock.lock();
            try {
                segments = segments.filter(segment -> !segment.isGarbage());
            } finally {
                segmentsLock.unlock();
            }
            logger.debug("end full GC");
        });
    }

    private class Segment {
        private final ArrayList<LogEntry> entries = new ArrayList<>();
        private final long offset;
        private final File logFile;
        private final DataOutputStream logOutput;
        private final AtomicInteger added = new AtomicInteger(0);

        private Segment(long offset) {
            this.offset = offset;
            this.logFile = fileService.resource(serviceName, "event." + offset + ".log");
            try {
                this.logOutput = fileService.output(logFile, true);

                if (logFile.length() > 0) {
                    try (DataInputStream stream = fileService.input(logFile)) {
                        if (stream.available() > 0) {
                            while (stream.available() > 0) {
                                entries.add(LogEntry.codec.read(stream));
                            }
                        }
                        if (!entries.isEmpty()) {
                            index.set(entries.get(entries.size() - 1).index() + 1);
                        }
                        added.set(entries.size());
                    }
                }
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        private LogEntry append(long id, Message event) {
            synchronized (entries) {
                if (isFull()) {
                    return null;
                }
                LogEntry logEntry = new LogEntry(index.incrementAndGet(), id, event);
                LogEntry.codec.write(logOutput, logEntry);
                entries.add(logEntry);
                added.incrementAndGet();
                return logEntry;
            }
        }

        private boolean isFull() {
            return added.get() == segmentSize;
        }

        private boolean isGarbage() {
            return isFull() && entries.isEmpty();
        }

        private void compact() {
            synchronized (entries) {
                entries.removeIf(compactionFilter);
            }
        }

        private void close() {
            try {
                logOutput.close();
            } catch (IOException e) {
                throw new IOError(e);
            }
        }
    }
}
