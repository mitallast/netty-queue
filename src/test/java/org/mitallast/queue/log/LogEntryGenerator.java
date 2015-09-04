package org.mitallast.queue.log;

import org.mitallast.queue.common.UUIDs;
import org.mitallast.queue.log.entry.LogEntry;
import org.mitallast.queue.log.entry.TextLogEntry;

public class LogEntryGenerator {

    public static LogEntry[] generate(int max) {
        return generate(0, max);
    }

    public static LogEntry[] generate(int from, int max) {
        LogEntry[] logEntries = new LogEntry[max];
        for (int i = 0; i < max; i++) {
            logEntries[i] = TextLogEntry.builder()
                .setIndex(from + i + 1)
                .setMessage(UUIDs.generateRandom().toString())
                .build();
        }
        return logEntries;
    }
}
