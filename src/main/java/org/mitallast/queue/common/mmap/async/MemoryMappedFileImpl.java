package org.mitallast.queue.common.mmap.async;

import io.netty.buffer.ByteBuf;
import org.mitallast.queue.common.concurrent.futures.Futures;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;
import org.mitallast.queue.common.mmap.MemoryMappedPage;
import org.mitallast.queue.common.mmap.MemoryMappedPageCacheLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MemoryMappedFileImpl implements MemoryMappedFile, MemoryMappedPageCacheLoader {

    public final static int DEFAULT_PAGE_SIZE = 1048576;
    public final static int DEFAULT_MAX_PAGES = 64;
    private final static Logger logger = LoggerFactory.getLogger(MemoryMappedFileImpl.class);
    private final File file;
    private final RandomAccessFile randomAccessFile;
    private final int pageSize;
    private final FileChannel channel;
    private final MemoryMappedPageCache pageCache;

    public MemoryMappedFileImpl(File file) throws IOException {
        this(file, DEFAULT_PAGE_SIZE, DEFAULT_MAX_PAGES);
    }

    public MemoryMappedFileImpl(File file, int pageSize, int maxPages) throws IOException {
        this.file = file;
        this.randomAccessFile = new RandomAccessFile(file, "rw");
        this.pageSize = pageSize;
        channel = randomAccessFile.getChannel();
        pageCache = new MemoryMappedPageCacheImpl(this, maxPages);
    }

    public File getFile() {
        return file;
    }

    @Override
    public SmartFuture<Boolean> putLong(long offset, long value) {
        SmartFuture<Boolean> future = Futures.future();
        getPage(offset).on(result -> {
            if (result.isError()) {
                future.invokeException(result.getError());
                return;
            }
            MemoryMappedPage page = result.getOrNull();
            long pageMax = page.getOffset() + pageSize;
            try {
                if (offset + 8 < pageMax) {
                    page.putLong(offset, value);
                    future.invoke(true);
                } else {
                    page.putInt(offset, (int) (value >>> 32));
                }
            } finally {
                try {
                    releasePage(page);
                } catch (IOException e) {
                    future.invokeException(e);
                }
            }
            if (offset + 8 >= pageMax) {
                putInt(offset + 4, (int) value).on(intResult -> {
                    if (result.isError()) {
                        future.invokeException(result.getError());
                        return;
                    }
                    future.invoke(intResult.getOrDefault(false));
                });
            }
        });
        return future;
    }

    @Override
    public SmartFuture<Long> getLong(long offset) throws IOException {
        SmartFuture<Long> future = Futures.future();
        getPage(offset).on(result -> {
            if (result.isError()) {
                future.invokeException(result.getError());
                return;
            }
            final long value;
            MemoryMappedPage page = result.getOrNull();
            long pageMax = page.getOffset() + pageSize;
            try {
                if (offset + 8 < pageMax) {
                    value = page.getLong(offset);
                    future.invoke(value);
                } else {
                    value = (page.getInt(offset) & 0xffffffffL) << 32;
                }
            } finally {
                try {
                    releasePage(page);
                } catch (IOException e) {
                    future.invokeException(e);
                }
            }
            if (offset + 8 >= pageMax) {
                getInt(offset + 4).on(intResult -> {
                    if (result.isError()) {
                        future.invokeException(result.getError());
                        return;
                    }
                    future.invoke(value | intResult.getOrDefault(0) & 0xffffffffL);
                });
            }
        });
        return future;
    }

    @Override
    public SmartFuture<Boolean> putInt(long offset, int value) {
        SmartFuture<Boolean> future = Futures.future();
        getPage(offset).on(result -> {
            if (result.isError()) {
                future.invokeException(result.getError());
                return;
            }
            MemoryMappedPage page = result.getOrNull();
            try {
                page.putInt(offset, value);
                future.invoke(true);
            } finally {
                try {
                    releasePage(page);
                } catch (IOException e) {
                    future.invokeException(e);
                }
            }
        });
        return future;
    }

    @Override
    public SmartFuture<Integer> getInt(long offset) {
        SmartFuture<Integer> future = Futures.future();
        getPage(offset).on(result -> {
            if (result.isError()) {
                future.invokeException(result.getError());
                return;
            }
            MemoryMappedPage page = result.getOrNull();
            try {
                future.invoke(page.getInt(offset));
            } finally {
                try {
                    releasePage(page);
                } catch (IOException e) {
                    future.invokeException(e);
                }
            }
        });
        return future;
    }

    @Override
    public SmartFuture<Boolean> getBytes(long offset, byte[] data) throws IOException {
        return getBytes(offset, data, 0, data.length);
    }

    @Override
    public SmartFuture<Boolean> getBytes(long offset, ByteBuf buffer, int length) throws IOException {
        SmartFuture<Boolean> future = Futures.future();
        long position = offset;
        long end = offset + length;
        int data_position = 0;

        List<SmartFuture<MemoryMappedPage>> futures = new ArrayList<>(2);
        while (position < end) {
            int max = Math.min(
                pageSize - (int) (position % pageSize),
                length - data_position);
            long current_pos = position;
            int current_data_pos = data_position;
            data_position += max;
            position += max;
            SmartFuture<MemoryMappedPage> pageFuture = getPage(current_pos);
            pageFuture.on(result -> {
                if (result.isError()) {
                    future.invokeException(result.getError());
                    return;
                }
                MemoryMappedPage page = result.getOrNull();
                try {
                    page.getBytes(current_pos, buffer, current_data_pos, max);
                } finally {
                    try {
                        releasePage(page);
                    } catch (IOException e) {
                        future.invokeException(e);
                    }
                }
            });
            futures.add(pageFuture);
        }
        AtomicInteger counter = new AtomicInteger(futures.size());
        futures.forEach(pageFuture -> pageFuture.on(result -> {
            if (result.isError()) {
                future.invokeException(result.getError());
                return;
            }
            if (counter.decrementAndGet() == 0) {
                buffer.writerIndex(buffer.writerIndex() + length);
                future.invoke(true);
            }
        }));
        return future;
    }

    @Override
    public SmartFuture<Boolean> getBytes(long offset, byte[] data, int start, int length) throws IOException {
        SmartFuture<Boolean> future = Futures.future();
        long position = offset;
        long end = offset + length;
        int data_position = start;

        List<SmartFuture<MemoryMappedPage>> futures = new ArrayList<>(2);
        while (position < end) {
            int max = Math.min(
                pageSize - (int) (position % pageSize),
                length - data_position);
            long current_pos = position;
            int current_data_pos = data_position;
            data_position += max;
            position += max;
            SmartFuture<MemoryMappedPage> pageFuture = getPage(current_pos);
            pageFuture.on(result -> {
                if (result.isError()) {
                    future.invokeException(result.getError());
                    return;
                }
                MemoryMappedPage page = result.getOrNull();
                try {
                    page.getBytes(current_pos, data, current_data_pos, max);
                } finally {
                    try {
                        releasePage(page);
                    } catch (IOException e) {
                        future.invokeException(e);
                    }
                }
            });
            futures.add(pageFuture);
        }
        AtomicInteger counter = new AtomicInteger(futures.size());
        futures.forEach(pageFuture -> pageFuture.on(result -> {
            if (result.isError()) {
                future.invokeException(result.getError());
                return;
            }
            if (counter.decrementAndGet() == 0) {
                future.invoke(true);
            }
        }));
        return future;
    }

    @Override
    public SmartFuture<Boolean> putBytes(long offset, byte[] data) throws IOException {
        return putBytes(offset, data, 0, data.length);
    }

    @Override
    public SmartFuture<Boolean> putBytes(long offset, ByteBuf buffer) throws IOException {
        return putBytes(offset, buffer, buffer.readableBytes());
    }

    @Override
    public SmartFuture<Boolean> putBytes(long offset, ByteBuf buffer, int length) throws IOException {
        SmartFuture<Boolean> future = Futures.future();
        long position = offset;
        long end = offset + length;
        int data_position = 0;

        List<SmartFuture<MemoryMappedPage>> futures = new ArrayList<>(2);
        while (position < end) {
            int max = Math.min(
                pageSize - (int) (position % pageSize),
                length - data_position);
            long current_pos = position;
            int current_data_pos = data_position;
            data_position += max;
            position += max;
            SmartFuture<MemoryMappedPage> pageFuture = getPage(current_pos);
            pageFuture.on(result -> {
                if (result.isError()) {
                    future.invokeException(result.getError());
                    return;
                }
                MemoryMappedPage page = result.getOrNull();
                try {
                    page.putBytes(current_pos, buffer, current_data_pos, max);
                } finally {
                    try {
                        releasePage(page);
                    } catch (IOException e) {
                        future.invokeException(e);
                    }
                }
            });
            futures.add(pageFuture);
        }
        AtomicInteger counter = new AtomicInteger(futures.size());
        futures.forEach(pageFuture -> pageFuture.on(result -> {
            if (result.isError()) {
                future.invokeException(result.getError());
                return;
            }
            if (counter.decrementAndGet() == 0) {
                buffer.readerIndex(buffer.readerIndex() + length);
                future.invoke(true);
            }
        }));
        return future;
    }

    @Override
    public SmartFuture<Boolean> putBytes(long offset, byte[] data, int start, int length) throws IOException {
        SmartFuture<Boolean> future = Futures.future();
        long position = offset;
        long end = offset + length;
        int data_position = start;

        List<SmartFuture<MemoryMappedPage>> futures = new ArrayList<>(2);
        while (position < end) {
            int max = Math.min(
                pageSize - (int) (position % pageSize),
                length - data_position);
            long current_pos = position;
            int current_data_pos = data_position;
            data_position += max;
            position += max;
            SmartFuture<MemoryMappedPage> pageFuture = getPage(current_pos);
            pageFuture.on(result -> {
                if (result.isError()) {
                    future.invokeException(result.getError());
                    return;
                }
                MemoryMappedPage page = result.getOrNull();
                try {
                    page.putBytes(current_pos, data, current_data_pos, max);
                } finally {
                    try {
                        releasePage(page);
                    } catch (IOException e) {
                        future.invokeException(e);
                    }
                }
            });
            futures.add(pageFuture);
        }
        AtomicInteger counter = new AtomicInteger(futures.size());
        futures.forEach(pageFuture -> pageFuture.on(result -> {
            if (result.isError()) {
                future.invokeException(result.getError());
                return;
            }
            if (counter.decrementAndGet() == 0) {
                future.invoke(true);
            }
        }));
        return future;
    }

    @Override
    public long length() throws IOException {
        return randomAccessFile.length();
    }

    @Override
    public boolean isEmpty() throws IOException {
        return randomAccessFile.length() == 0;
    }

    @Override
    public void flush() throws IOException {
        pageCache.flush();
    }

    @Override
    public void close() throws IOException {
        pageCache.close();
        channel.close();
        randomAccessFile.close();
    }

    @Override
    public void delete() throws IOException {
        close();
        if (file.exists() && !file.delete()) {
            throw new IOException("Error delete file " + file);
        }
    }

    public SmartFuture<MemoryMappedPage> getPage(long offset) {
        long pageOffset = (offset / pageSize) * pageSize;
        return pageCache.acquire(pageOffset);
    }

    public void releasePage(MemoryMappedPage page) throws IOException {
        pageCache.release(page);
    }

    @Override
    public MemoryMappedPage load(long offset) throws IOException {
        synchronized (channel) {
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, offset, pageSize);
            buffer.load();
            buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);
            return new MemoryMappedPage(buffer, offset);
        }
    }
}
