package org.mitallast.queue.crdt;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import javaslang.collection.HashMap;
import javaslang.collection.HashSet;
import javaslang.collection.Vector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.file.FileService;
import org.mitallast.queue.common.stream.*;
import org.mitallast.queue.crdt.log.FileReplicatedLog;
import org.mitallast.queue.crdt.log.LogEntry;

public class FileReplicatedLogTest extends BaseTest {

    InternalStreamService streamService;
    Config config;
    FileReplicatedLog log;

    @Before
    public void setUp() throws Exception {
        streamService = new InternalStreamService(HashSet.of(
            StreamableRegistry.of(TestLong.class, TestLong::new, 1)
        ).toJavaSet());
        config = ConfigFactory.parseMap(HashMap.of("node.path", testFolder.newFolder().getAbsolutePath()).toJavaMap())
            .withFallback(ConfigFactory.defaultReference());
        log = new FileReplicatedLog(
            config,
            new FileService(
                config,
                streamService
            ),
            streamService,
            logEntry -> false,
            0,
            0
        );
    }

    @Test
    public void append() throws Exception {
        long total = 4000000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < total; i++) {
            LogEntry append = log.append(i, new TestLong(i));
            assert append.index() == i + 1;
            assert append.id() == i;
        }
        long end = System.currentTimeMillis();
        printQps("append single thread", total, start, end);
        Vector<LogEntry> logEntries = log.entriesFrom(0);
        assert logEntries.size() == total;
        long prev = 1;
        for (LogEntry logEntry : logEntries) {
            Assert.assertEquals(prev, logEntry.index());
            prev++;
        }
    }

    @Test
    public void appendConcurrent() throws Exception {
        long total = 4000000;
        long start = System.currentTimeMillis();
        executeConcurrent((thread, concurrency) -> {
            for (int i = thread; i < total; i += concurrency) {
                LogEntry append = log.append(i, new TestLong(i));
                assert append.id() == i;
            }
        });
        long end = System.currentTimeMillis();
        printQps("append concurrent", total, start, end);
        assert log.entriesFrom(0).size() == total;
        long prev = 1;
        for (LogEntry logEntry : log.entriesFrom(0)) {
            Assert.assertEquals(prev, logEntry.index());
            prev++;
        }
    }

    public static class TestLong implements Streamable {
        private final long value;

        public TestLong(long value) {
            this.value = value;
        }

        public TestLong(StreamInput stream) {
            this.value = stream.readLong();
        }

        @Override
        public void writeTo(StreamOutput stream) {
            stream.writeLong(value);
        }
    }
}
