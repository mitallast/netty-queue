package org.mitallast.queue.crdt;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.vavr.collection.HashMap;
import io.vavr.collection.Vector;
import org.apache.logging.log4j.MarkerManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.common.file.FileService;
import org.mitallast.queue.common.logging.LoggingService;
import org.mitallast.queue.crdt.log.FileReplicatedLog;
import org.mitallast.queue.crdt.log.LogEntry;

public class FileReplicatedLogTest extends BaseTest {

    static {
        Codec.Companion.register(777777, TestLong.class, TestLong.codec);
    }

    Config config;
    LoggingService logging;
    FileReplicatedLog log;

    @Before
    public void setUp() throws Exception {
        config = ConfigFactory.parseMap(HashMap.of("node.path", testFolder.newFolder().getAbsolutePath()).toJavaMap())
            .withFallback(ConfigFactory.defaultReference());
        logging = new LoggingService(MarkerManager.getMarker("test"));
        log = new FileReplicatedLog(
            logging,
            config,
            new FileService(config),
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
            assert append.getIndex() == i + 1;
            assert append.getId() == i;
        }
        long end = System.currentTimeMillis();
        printQps("append single thread", total, start, end);
        Vector<LogEntry> logEntries = log.entriesFrom(0);
        assert logEntries.size() == total;
        long prev = 1;
        for (LogEntry logEntry : logEntries) {
            Assert.assertEquals(prev, logEntry.getIndex());
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
                assert append.getId() == i;
            }
        });
        long end = System.currentTimeMillis();
        printQps("append concurrent", total, start, end);
        assert log.entriesFrom(0).size() == total;
        long prev = 1;
        for (LogEntry logEntry : log.entriesFrom(0)) {
            Assert.assertEquals(prev, logEntry.getIndex());
            prev++;
        }
    }

    public static class TestLong implements Message {
        public static final Codec<TestLong> codec = Codec.Companion.of(
            TestLong::new,
            TestLong::value,
            Codec.Companion.longCodec()
        );

        private final long value;

        public TestLong(long value) {
            this.value = value;
        }

        public long value() {
            return value;
        }
    }
}
