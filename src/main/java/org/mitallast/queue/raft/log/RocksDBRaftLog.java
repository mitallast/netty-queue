package org.mitallast.queue.raft.log;

import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.mitallast.queue.common.Longs;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.raft.log.entry.RaftLogEntry;
import org.rocksdb.*;

import java.io.File;
import java.io.IOException;

public class RocksDBRaftLog extends AbstractLifecycleComponent implements RaftLog {
    private final StreamService streamService;

    private volatile RocksDBExtended rocksDB;
    private volatile Options options;
    private volatile long skip;
    private long firstIndex;
    private long lastIndex;

    @Inject
    public RocksDBRaftLog(Settings settings, StreamService streamService) {
        super(settings);
        this.streamService = streamService;
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
        options = new Options().setCreateIfMissing(true);
        try {
            rocksDB = RocksDBExtended.open(options, directory.toString());
        } catch (RocksDBException e) {
            throw new IOException(e);
        }

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
    }

    @Override
    public long firstIndex() {
        checkIsStarted();
        return firstIndex;
    }

    @Override
    public long nextIndex() {
        return lastIndex() + 1;
    }

    @Override
    public long lastIndex() {
        checkIsStarted();
        return lastIndex;
    }

    @Override
    public long appendEntry(RaftLogEntry entry) throws IOException {
        checkIsStarted();
        final long index = nextIndex();
        if (entry.index() != index) {
            throw new IndexOutOfBoundsException("inconsistent index: " + entry.index() + " entry: " + entry);
        }
        EntryBuilder entryBuilder = entry.toBuilder();
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer();
        buffer.readerIndex(0);
        buffer.writerIndex(0);
        try (StreamOutput output = streamService.output(buffer)) {
            output.writeClass(entryBuilder.getClass());
            output.writeStreamable(entryBuilder);
        }
        int len = buffer.readableBytes();
        byte[] bytes = new byte[len];
        buffer.readBytes(bytes);
        try {
            rocksDB.put(Longs.toBytes(index), bytes);
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
            byte[] bytes = rocksDB.get(Longs.toBytes(index));
            if (bytes == null || bytes.length == 0) {
                return null;
            }
            try (StreamInput input = streamService.input(Unpooled.wrappedBuffer(bytes))) {
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
        return rocksDB.keyMayExist(Longs.toBytes(index), new StringBuffer(0));
    }

    @Override
    public boolean containsEntry(long index) throws IOException {
        checkIsStarted();
        return rocksDB.keyMayExist(Longs.toBytes(index), new StringBuffer(0));
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
