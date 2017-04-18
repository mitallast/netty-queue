package org.mitallast.queue.crdt.log;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.typesafe.config.Config;
import javaslang.collection.Vector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.common.file.FileService;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.common.stream.Streamable;

import java.io.File;
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
    private final String serviceName;

    private final ExecutorService compaction = Executors.newSingleThreadExecutor();
    private final ReentrantLock segmentsLock = new ReentrantLock();
    private volatile Vector<Segment> segments = Vector.empty();
    private volatile Segment lastSegment;

    private final AtomicLong vclock = new AtomicLong(0);

    @Inject
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public FileReplicatedLog(
        Config config,
        FileService fileService,
        StreamService streamService,
        @Assisted Predicate<LogEntry> compactionFilter,
        @Assisted int index,
        @Assisted long replica
    ) {
        this.segmentSize = config.getInt("crdt.segment.size");

        this.fileService = fileService;
        this.streamService = streamService;
        this.compactionFilter = compactionFilter;
        this.serviceName = String.format("crdt/%d/log/%d", index, replica);

        fileService.service(serviceName).mkdir();
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
            segments = segments.append(new Segment(vclock.get()));
        }
        lastSegment = segments.get(segments.size() - 1);
    }

    @Override
    public long vclock() {
        return vclock.get();
    }

    @Override
    public LogEntry append(long id, Streamable event) {
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
    public Vector<LogEntry> entriesFrom(long nodeVclock) {
        Vector<LogEntry> builder = Vector.empty();
        for (Segment segment : segments.reverse()) {
            synchronized (segment.entries) {
                for (int i = segment.entries.size() - 1; i >= 0; i--) {
                    LogEntry logEntry = segment.entries.get(i);
                    if (logEntry.vclock() > nodeVclock) {
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
        private final StreamOutput logOutput;

        private volatile int added = 0;

        private Segment(long offset) {
            this.offset = offset;
            this.logFile = fileService.resource(serviceName, "event." + offset + ".log");
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

        private LogEntry append(long id, Streamable event) {
            synchronized (entries) {
                if (isFull()) {
                    return null;
                }
                LogEntry logEntry = new LogEntry(vclock.incrementAndGet(), id, event);
                logOutput.writeStreamable(logEntry);
                entries.add(logEntry);
                added = added + 1;
                return logEntry;
            }
        }

        private boolean isFull() {
            return added == segmentSize;
        }

        private boolean isGarbage() {
            return added == segmentSize && entries.isEmpty();
        }

        private void compact() {
            synchronized (entries) {
                entries.removeIf(compactionFilter);
            }
        }

        private void close() {
            logOutput.close();
        }
    }
}
