package org.mitallast.queue.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.stream.InternalStreamService;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.log.LogEntryGenerator;
import org.mitallast.queue.log.LogStreamService;
import org.mitallast.queue.log.entry.LogEntry;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBExtended;
import org.rocksdb.RocksIterator;

public class RocksDBBenchmark extends BaseTest {

    private Options options;
    private RocksDBExtended rocksDB;

    private StreamService streamService;

    private byte[] keyBuffer = new byte[Long.BYTES];
    private byte[] valueBuffer = new byte[64];

    @Override
    protected int max() {
        return 500_000;
    }

    @Before
    public void setUp() throws Exception {
        RocksDB.loadLibrary();
        options = new Options().setCreateIfMissing(true);
        rocksDB = RocksDBExtended.open(options, testFolder.getRoot().toString());

        streamService = new InternalStreamService(ImmutableSettings.EMPTY);
        new LogStreamService(streamService);
    }

    @After
    public void tearDown() throws Exception {
        if (rocksDB != null) {
            rocksDB.close();
            rocksDB = null;
        }
        if (options != null) {
            options.dispose();
            options = null;
        }
    }

    @Test
    public void testBenchmark() throws Exception {
        LogEntry[] generate = LogEntryGenerator.generate(max());
        ByteBuf buffer = Unpooled.wrappedBuffer(valueBuffer);
        StreamOutput output = streamService.output(buffer);

        long start = System.currentTimeMillis();
        for (LogEntry logEntry : generate) {
            buffer.writerIndex(0);
            LogEntry.Builder builder = logEntry.toBuilder();
            output.writeClass(builder.getClass());
            output.writeStreamable(builder);
            int len = buffer.writerIndex();
            Longs.toBytes(keyBuffer, logEntry.index());
            rocksDB.put(keyBuffer, valueBuffer, len);
        }
        long end = System.currentTimeMillis();
        printQps("put", max(), start, end);

        start = System.currentTimeMillis();
        StreamInput input = streamService.input(buffer);
        for (LogEntry logEntry : generate) {
            Longs.toBytes(keyBuffer, logEntry.index());
            int read = rocksDB.get(keyBuffer, valueBuffer);
            buffer.readerIndex(0);
            buffer.writerIndex(read);
            LogEntry.Builder builder = input.readStreamable();
        }
        end = System.currentTimeMillis();
        printQps("get", max(), start, end);


        start = System.currentTimeMillis();
        int count = 0;
        RocksIterator iterator = rocksDB.newIterator();
        try {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                count++;
            }
        } finally {
            iterator.dispose();
        }
        assert count == max();
        end = System.currentTimeMillis();
        printQps("iterate", max(), start, end);
    }


}
