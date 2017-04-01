package org.mitallast.queue.crdt.log;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.common.file.FileService;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.common.stream.Streamable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class FileReplicatedLog implements ReplicatedLog {

    private final static Logger logger = LogManager.getLogger();

    private final int thresholdAdded;
    private final int thresholdCompacted;

    private final FileService fileService;
    private final StreamService streamService;
    private final Predicate<LogEntry> compactionFilter;

    private final ArrayList<LogEntry> entries = new ArrayList<>();

    private volatile File logFile;
    private volatile StreamOutput logOutput;

    private volatile int added = 0;
    private volatile int compacted = 0;

    @Inject
    public FileReplicatedLog(
        Config config,
        FileService fileService,
        StreamService streamService,
        Predicate<LogEntry> compactionFilter
    ) throws IOException {
        this.thresholdAdded = config.getInt("crdt.compaction.threshold.added");
        this.thresholdCompacted = config.getInt("crdt.compaction.threshold.compacted");

        this.fileService = fileService;
        this.streamService = streamService;
        this.compactionFilter = compactionFilter;

        logFile = fileService.resource("crdt", "event.log");
        try (StreamInput input = streamService.input(logFile)) {
            while (input.available() > 0) {
                entries.add(input.readStreamable(LogEntry::new));
            }
            logOutput = streamService.output(logFile, true);
        }
    }

    @Override
    public LogEntry append(long id, Streamable event) throws IOException {
        final long vclock;
        if (entries.isEmpty()) {
            vclock = 1;
        } else {
            vclock = entries.get(entries.size() - 1).vclock() + 1;
        }
        LogEntry logEntry = new LogEntry(vclock, id, event);
        append(logEntry);
        added++;
        compact();
        return logEntry;
    }

    @Override
    public void append(LogEntry logEntry) throws IOException {
        logOutput.writeStreamable(logEntry);
        entries.add(logEntry);
    }

    public List<LogEntry> entries() {
        return entries;
    }

    private void compact() throws IOException {
        if (added >= thresholdAdded) {
            int before = entries.size();
            entries.removeIf(compactionFilter);
            compacted = compacted + (before - entries.size());

            logger.debug("added {} compacted {}", added, compacted);
            if (compacted >= thresholdCompacted) {
                this.logOutput.close();
                long beforeSize = logFile.length();
                File compactedFile = fileService.temporary("crdt", "event", "log");
                try (StreamOutput stream = streamService.output(compactedFile)) {
                    for (LogEntry logEntry : entries) {
                        stream.writeStreamable(logEntry);
                    }
                }

                Files.move(compactedFile.toPath(), logFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

                logFile = fileService.resource("crdt", "event.log");
                logOutput = streamService.output(logFile, true);
                logger.info("compacted log file: before {} after {} bytes", beforeSize, logFile.length());
                compacted = 0;
            }
        }
    }
}
