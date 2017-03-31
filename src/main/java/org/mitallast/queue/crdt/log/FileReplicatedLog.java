package org.mitallast.queue.crdt.log;

import com.google.inject.Inject;
import org.mitallast.queue.common.file.FileService;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.common.stream.Streamable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileReplicatedLog implements ReplicatedLog {

    private final File logFile;
    private final StreamOutput logOutput;
    private final ArrayList<LogEntry> entries = new ArrayList<>();

    @Inject
    public FileReplicatedLog(FileService fileService, StreamService streamService) throws IOException {
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
}
