package org.mitallast.queue.crdt.log;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.common.Immutable;
import org.mitallast.queue.common.file.FileService;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.common.stream.Streamable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

public class FileReplicatedLog implements ReplicatedLog {

    private final static Logger logger = LogManager.getLogger();

    private final int segmentSize;

    private final FileService fileService;
    private final StreamService streamService;
    private final Predicate<LogEntry> compactionFilter;

    private final ExecutorService compaction = Executors.newSingleThreadExecutor();
    private final ReentrantLock segmentsLock = new ReentrantLock();
    private volatile ImmutableList<Segment> segments = ImmutableList.of();
    private volatile Segment lastSegment;

    private final AtomicLong vclock = new AtomicLong(0);

    @Inject
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public FileReplicatedLog(
        Config config,
        FileService fileService,
        StreamService streamService,
        Predicate<LogEntry> compactionFilter
    ) throws IOException {
        this.segmentSize = config.getInt("crdt.segment.size");

        this.fileService = fileService;
        this.streamService = streamService;
        this.compactionFilter = compactionFilter;

        fileService.service("crdt").mkdir();
        long[] offsets = fileService.resources("crdt", "regex:event.[0-9]+.log")
            .map(path -> path.getFileName().toString())
            .map(name -> name.substring(5, name.length() - 5))
            .mapToLong(Long::parseLong)
            .sorted()
            .toArray();

        for (int i = 0; i < offsets.length; i++) {
            segments = Immutable.append(segments, new Segment(i));
        }
        if (segments.isEmpty()) {
            segments = Immutable.append(segments, new Segment(vclock.get()));
        }
        lastSegment = segments.get(segments.size() - 1);
    }

    @Override
    public LogEntry append(long id, Streamable event) throws IOException {
        while (true) {
            LogEntry append = lastSegment.append(id, event);
            if (append != null) {
                return append;
            }
            boolean startGC = false;
            segmentsLock.lock();
            try {
                if (lastSegment.isFull()) {
                    lastSegment = new Segment(vclock.get());
                    segments = Immutable.append(segments, lastSegment);
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
    public ImmutableList<LogEntry> entriesFrom(long nodeVclock) {
        ImmutableList.Builder<LogEntry> builder = null;
        for (Segment segment : segments.reverse()) {
            for (int i = segment.entries.size() - 1; i >= 0; i--) {
                LogEntry logEntry = segment.entries.get(i);
                if (logEntry.vclock() > nodeVclock) {
                    if (builder == null) {
                        builder = ImmutableList.builder();
                    }
                    builder.add(logEntry);
                } else {
                    if (builder == null) {
                        return ImmutableList.of();
                    } else {
                        return builder.build().reverse();
                    }
                }
            }
        }
        if (builder == null) {
            return ImmutableList.of();
        } else {
            return builder.build().reverse();
        }
    }

    void startGC() {
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
                        try {
                            segment.close();
                        } catch (IOException e) {
                            logger.error("error close segment", e);
                        }
                        try {
                            Files.deleteIfExists(segment.logFile.toPath());
                        } catch (IOException e) {
                            logger.error("error delete segment file", e);
                        }
                    }
                }
            }
            segmentsLock.lock();
            try {
                segments = Immutable.filterNot(segments, Segment::isGarbage);
            } finally {
                segmentsLock.unlock();
            }
            logger.debug("end full GC");
        });
    }

    class Segment {
        private final ReentrantLock lock = new ReentrantLock();
        private final ArrayList<LogEntry> entries = new ArrayList<>();
        private final long offset;
        private final File logFile;
        private final StreamOutput logOutput;

        private volatile int added = 0;

        private Segment(long offset) throws IOException {
            this.offset = offset;
            this.logFile = fileService.resource("crdt", "event." + offset + ".log");
            this.logOutput = streamService.output(logFile, true);

            if (logFile.length() > 0) {
                try (StreamInput input = streamService.input(logFile)) {
                    while (input.available() > 0) {
                        entries.add(input.readStreamable(LogEntry::new));
                    }
                    if (!entries.isEmpty()) {
                        vclock.set(entries.get(entries.size() - 1).vclock() + 1);
                    }
                    added = entries.size();
                }
            }
        }

        private LogEntry append(long id, Streamable event) throws IOException {
            lock.lock();
            try {
                if (isFull()) {
                    return null;
                }
                LogEntry logEntry = new LogEntry(vclock.incrementAndGet(), id, event);
                logOutput.writeStreamable(logEntry);
                entries.add(logEntry);
                added = added + 1;
                compact();
                return logEntry;
            } finally {
                lock.unlock();
            }
        }

        private boolean isFull() {
            return added == segmentSize;
        }

        private boolean isGarbage() {
            return added == segmentSize && entries.isEmpty();
        }

        private void compact() {
            lock.lock();
            try {
                entries.removeIf(compactionFilter);
            } finally {
                lock.unlock();
            }
        }

        private void close() throws IOException {
            logOutput.close();
        }
    }
}
