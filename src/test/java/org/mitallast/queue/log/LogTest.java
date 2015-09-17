package org.mitallast.queue.log;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseIntegrationTest;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.stream.InternalStreamService;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.log.entry.LogEntry;
import org.mitallast.queue.log.entry.TextLogEntry;
import org.unitils.reflectionassert.ReflectionAssert;

public class LogTest extends BaseIntegrationTest {

    private LogService logService;
    private Log log;

    @Override
    protected int max() {
        return 10_000_000;
    }

    @Before
    public void setUp() throws Exception {
        Settings settings = settings();
        StreamService streamService = new InternalStreamService(settings);
        new LogStreamService(streamService);
        logService = new LogService(settings, streamService);
        logService.start();
        log = logService.log("test log");
    }

    @After
    public void tearDown() throws Exception {
        logService.stop();
        logService.close();
    }

    @Test
    public void testAppend() throws Exception {
        long id1 = log.nextIndex();
        TextLogEntry entry1 = TextLogEntry.builder()
            .setIndex(id1)
            .setMessage("hello world")
            .build();
        log.appendEntry(entry1);

        long id2 = log.nextIndex();
        TextLogEntry entry2 = TextLogEntry.builder()
            .setIndex(id2)
            .setMessage("hello world")
            .build();
        log.appendEntry(entry2);

        LogEntry saved1 = log.getEntry(id1);
        LogEntry saved2 = log.getEntry(id2);

        ReflectionAssert.assertReflectionEquals(entry1, saved1);
        ReflectionAssert.assertReflectionEquals(entry2, saved2);
    }

    @Test
    public void bench() throws Exception {
        LogEntry[] entries = LogEntryGenerator.generate(max());

        long start = System.currentTimeMillis();
        for (LogEntry entry : entries) {
            log.appendEntry(entry);
        }
        long end = System.currentTimeMillis();
        printQps("append entry", max(), start, end);

        start = System.currentTimeMillis();
        for (LogEntry entry : entries) {
            log.getEntry(entry.index());
        }
        end = System.currentTimeMillis();
        printQps("get entry", max(), start, end);

        start = System.currentTimeMillis();
        for (LogEntry entry : entries) {
            log.containsEntry(entry.index());
        }
        end = System.currentTimeMillis();
        printQps("contains entry", max(), start, end);
    }
}
