package org.mitallast.queue.raft.log;

import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.mitallast.queue.common.Longs;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.common.unit.ByteSizeUnit;
import org.mitallast.queue.raft.log.entry.RaftLogEntry;
import org.rocksdb.*;

import java.io.File;
import java.io.IOException;

public class RocksDBRaftLog extends AbstractLifecycleComponent implements RaftLog {
    private final StreamService streamService;

    private final byte[] keyBuffer;
    private final ByteBuf valueBuffer;
    private final StreamOutput output;
    private final StringBuffer existsBuffer;

    private RocksDBExtended rocksDB;
    private Options options;
    private WriteOptions writeOptions;

    private long skip;
    private long firstIndex;
    private long lastIndex;

    @Inject
    public RocksDBRaftLog(Settings settings, StreamService streamService) {
        super(settings);
        this.streamService = streamService;

        keyBuffer = new byte[Long.BYTES];
        valueBuffer = Unpooled.buffer();
        output = streamService.output(valueBuffer);
        existsBuffer = new StringBuffer();
    }

    @Override
    protected void doStart() throws IOException {
        File workDir = new File(this.settings.get("work_dir", "data"));
        File directory = new File(workDir, componentSettings.get("log_dir", "raft"));
        if (!directory.exists()) {
            if (!directory.mkdir()) {
                throw new IOException("Error create directory: " + directory);
            }
        }

        RocksDB.loadLibrary();
        options = new Options();
        options.setCreateIfMissing(true);
        options.setParanoidChecks(false);

        options.useFixedLengthPrefixExtractor(Long.BYTES);

        BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
        blockBasedTableConfig.setChecksumType(ChecksumType.kCRC32c);
        blockBasedTableConfig.setIndexType(IndexType.kBinarySearch);
        blockBasedTableConfig.setBlockSize(ByteSizeUnit.MB.toBytes(4));
        blockBasedTableConfig.setBlockCacheSize(ByteSizeUnit.GB.toBytes(1));
        blockBasedTableConfig.setCacheNumShardBits(6);
        options.setTableFormatConfig(blockBasedTableConfig);

        options.useFixedLengthPrefixExtractor(Long.BYTES);

        options.setIncreaseParallelism(8);
        options.setAllowMmapReads(true);
        options.setAllowMmapWrites(true);
        options.setCompressionType(CompressionType.LZ4_COMPRESSION);
        options.setCompactionStyle(CompactionStyle.FIFO);
        options.setVerifyChecksumsInCompaction(false);

        options.setWriteBufferSize(ByteSizeUnit.MB.toBytes(64));
        options.setMaxWriteBufferNumber(4);
        options.setTargetFileSizeBase(ByteSizeUnit.MB.toBytes(64));
        options.setMaxBackgroundCompactions(4);
        options.setLevelZeroFileNumCompactionTrigger(8);
        options.setLevelZeroSlowdownWritesTrigger(17);
        options.setLevelZeroStopWritesTrigger(24);
        options.setNumLevels(6);
        options.setMaxBytesForLevelBase(ByteSizeUnit.MB.toBytes(512));
        options.setMaxBytesForLevelMultiplier(8);

        options.setLogger(new Logger(options) {
            @Override
            protected void log(InfoLogLevel infoLogLevel, String logMsg) {
                switch (infoLogLevel) {
                    case FATAL_LEVEL:
                    case ERROR_LEVEL:
                        logger.error("[rocksdb] {}", logMsg);
                        break;
                    case WARN_LEVEL:
                        logger.warn("[rocksdb] {}", logMsg);
                        break;
                    default:
                        logger.debug("[rocksdb] {}", logMsg);
                        break;
                }
            }
        });

        try {
            rocksDB = RocksDBExtended.open(options, directory.toString());
        } catch (RocksDBException e) {
            throw new IOException(e);
        }

        writeOptions = new WriteOptions();
        writeOptions.setDisableWAL(true);
        writeOptions.setSync(false);

        RocksIterator rocksIterator = null;
        try {
            rocksIterator = rocksDB.newIterator();
            rocksIterator.seekToFirst();
            if (rocksIterator.isValid()) {
                rocksIterator.next();
                byte[] firstKey = rocksIterator.key();
                lastIndex = firstIndex = Longs.fromBytes(firstKey);

                rocksIterator.seekToLast();
                if (rocksIterator.isValid()) {
                    rocksIterator.next();
                    byte[] lastKey = rocksIterator.key();
                    lastIndex = Longs.fromBytes(lastKey);
                }
            }
        } finally {
            if (rocksIterator != null) {
                rocksIterator.dispose();
            }
        }
    }

    @Override
    protected void doStop() throws IOException {
    }

    @Override
    protected void doClose() throws IOException {
        if (rocksDB != null) {
            rocksDB.close();
            rocksDB = null;
        }
        if (options != null) {
            options.dispose();
            options = null;
        }
        if (writeOptions != null) {
            writeOptions.dispose();
            writeOptions = null;
        }
    }

    @Override
    public long firstIndex() {
        return firstIndex;
    }

    @Override
    public long nextIndex() {
        return lastIndex() + 1;
    }

    @Override
    public long lastIndex() {
        return lastIndex + skip;
    }

    @Override
    public long appendEntry(RaftLogEntry entry) throws IOException {
        checkIsStarted();
        final long index = nextIndex();
        if (entry.index() != index) {
            throw new IndexOutOfBoundsException("inconsistent index: " + entry.index() + " entry: " + entry);
        }
        EntryBuilder entryBuilder = entry.toBuilder();

        valueBuffer.clear();
        output.writeClass(entryBuilder.getClass());
        output.writeStreamable(entryBuilder);
        try {
            Longs.toBytes(keyBuffer, index);
            rocksDB.put(writeOptions, keyBuffer, valueBuffer.array(), valueBuffer.readableBytes());
        } catch (RocksDBException e) {
            throw new IOException(e);
        }

        skip = 0;
        if (firstIndex == 0) {
            firstIndex = index;
        }
        lastIndex = index;
        return index;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends RaftLogEntry> T getEntry(long index) throws IOException {
        checkIsStarted();
        try {
            Longs.toBytes(keyBuffer, index);
            valueBuffer.clear();
            int read = rocksDB.get(keyBuffer, valueBuffer.array());
            valueBuffer.writerIndex(read);
            if (read <= 0) {
                return null;
            }
            try (StreamInput input = streamService.input(valueBuffer)) {
                RaftLogEntry.Builder builder = input.readStreamable();
                return (T) builder.build();
            }
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean containsIndex(long index) {
        checkIsStarted();
        existsBuffer.delete(0, existsBuffer.length());
        return rocksDB.keyMayExist(Longs.toBytes(index), existsBuffer);
    }

    @Override
    public boolean containsEntry(long index) throws IOException {
        checkIsStarted();
        existsBuffer.delete(0, existsBuffer.length());
        return rocksDB.keyMayExist(Longs.toBytes(index), existsBuffer);
    }

    @Override
    public void skip(long entries) throws IOException {
        checkIsStarted();
        skip += entries;
    }

    @Override
    public void truncate(long index) throws IOException {
        checkIsStarted();
        RocksIterator rocksIterator = null;
        try {
            rocksIterator = rocksDB.newIterator();
            for (rocksIterator.seek(Longs.toBytes(index)); rocksIterator.isValid(); rocksIterator.next()) {
                byte[] key = rocksIterator.key();
                rocksDB.remove(key);
            }
        } catch (RocksDBException e) {
            throw new IOException(e);
        } finally {
            if (rocksIterator != null) {
                rocksIterator.dispose();
            }
        }
    }

    @Override
    public void delete() throws IOException {
        checkIsStarted();
        close();
    }
}
