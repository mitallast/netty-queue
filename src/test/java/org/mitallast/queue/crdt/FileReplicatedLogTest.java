package org.mitallast.queue.crdt;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.file.FileService;
import org.mitallast.queue.common.stream.*;
import org.mitallast.queue.crdt.log.FileReplicatedLog;
import org.mitallast.queue.crdt.log.LogEntry;

import java.io.IOException;

public class FileReplicatedLogTest extends BaseTest {

    InternalStreamService streamService;
    Config config;
    FileReplicatedLog log;

    @Before
    public void setUp() throws Exception {
        streamService = new InternalStreamService(ImmutableSet.of(
            StreamableRegistry.of(TestLong.class, TestLong::new, 1)
        ));
        config = ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
            .put("node.path", testFolder.newFolder().getAbsolutePath())
            .build()).withFallback(ConfigFactory.defaultReference());
        log = new FileReplicatedLog(
            config,
            new FileService(
                config,
                streamService
            ),
            streamService,
            logEntry -> false,
            0
        );
    }

    @Test
    public void append() throws Exception {
        long total = 4000000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < total; i++) {
            LogEntry append = log.append(i, new TestLong(i));
            assert append.vclock() == i + 1;
            assert append.id() == i;
        }
        long end = System.currentTimeMillis();
        printQps("append single thread", total, start, end);
        ImmutableList<LogEntry> logEntries = log.entriesFrom(0);
        assert logEntries.size() == total;
        long prev = 1;
        for (LogEntry logEntry : logEntries) {
            Assert.assertEquals(prev, logEntry.vclock());
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
            Assert.assertEquals(prev, logEntry.vclock());
            prev++;
        }
    }

    public static class TestLong implements Streamable {
        private final long value;

        public TestLong(long value) {
            this.value = value;
        }

        public TestLong(StreamInput stream) throws IOException {
            this.value = stream.readLong();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeLong(value);
        }
    }
}
