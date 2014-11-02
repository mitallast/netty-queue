package org.mitallast.queue.queue.service.translog;

import com.google.common.cache.*;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class MemoryMappedFile implements Closeable {
    private final RandomAccessFile randomAccessFile;
    private final int pageSize;
    private final FileChannel channel;
    private final LoadingCache<Integer, MemoryMappedPage> cache;

    public MemoryMappedFile(RandomAccessFile randomAccessFile, int pageSize, int maxPages) throws IOException {
        this.randomAccessFile = randomAccessFile;
        this.pageSize = pageSize;
        channel = randomAccessFile.getChannel();
        cache = CacheBuilder.newBuilder()
            .maximumSize(maxPages)
            .concurrencyLevel(24)
            .removalListener(new RemovalListener<Integer, MemoryMappedPage>() {
                @Override
                public void onRemoval(RemovalNotification<Integer, MemoryMappedPage> entry) {
                    MemoryMappedPage page = entry.getValue();
                    if (page != null) {
                        page.release();
                    }
                }
            })
            .build(new CacheLoader<Integer, MemoryMappedPage>() {
                @Override
                public MemoryMappedPage load(Integer position) throws Exception {
                    MemoryMappedPage page = createPage(position);
                    page.acquire();
                    return page;
                }
            });
    }

    @Override
    public void close() throws IOException {
        cache.invalidateAll();
        channel.close();
        randomAccessFile.close();
    }

    public void putLong(long offset, long value) throws IOException {
        MemoryMappedPage page = getPage(offset);
        try {
            page.putLong(offset, value);
        } finally {
            releasePage(page);
        }
    }

    public long getLong(long offset) throws IOException {
        MemoryMappedPage page = getPage(offset);
        try {
            return page.getLong(offset);
        } finally {
            releasePage(page);
        }
    }

    public void putInt(long offset, int value) throws IOException {
        MemoryMappedPage page = getPage(offset);
        try {
            page.putInt(offset, value);
        } finally {
            releasePage(page);
        }
    }

    public int getInt(long offset) throws IOException {
        MemoryMappedPage page = getPage(offset);
        try {
            return page.getInt(offset);
        } finally {
            releasePage(page);
        }
    }

    public void getBytes(long offset, byte[] data) throws IOException {
        getBytes(offset, data, 0, data.length);
    }

    public void getBytes(final long offset, byte[] data, final int start, final int length) throws IOException {

        long position = offset;
        long end = offset + length;
        int dataPosition = start;
        while (position < end) {
            int max = pageSize - (int) (position % pageSize);
            max = Math.min(max, length - dataPosition);
            MemoryMappedPage page = getPage(position);
            try {
                page.getBytes(position, data, dataPosition, max);
            } finally {
                releasePage(page);
            }
            dataPosition += max;
            position += max;
        }
    }

    public void putBytes(long offset, byte[] data) throws IOException {
        putBytes(offset, data, 0, data.length);
    }

    public void putBytes(final long offset, final byte[] data, final int start, final int length) throws IOException {
        long position = offset;
        long end = offset + length;
        int dataPosition = start;
        while (position < end) {
            int max = pageSize - (int) (position % pageSize);
            max = Math.min(max, length - dataPosition);
            MemoryMappedPage page = getPage(position);
            try {
                page.putBytes(position, data, dataPosition, max);
            } finally {
                releasePage(page);
            }
            dataPosition += max;
            position += max;
        }
    }

    public long length() throws IOException {
        return randomAccessFile.length();
    }

    public void flush() throws IOException {
        for (MemoryMappedPage page : cache.asMap().values()) {
            page.flush();
        }
    }

    public MemoryMappedPage getPage(long offset) throws IOException {
        int index = (int) offset / pageSize;
        try {
            synchronized (cache) {
                MemoryMappedPage page = cache.get(index);
                page.acquire();
                return page;
            }
        } catch (ExecutionException e) {
            throw new IOException(e);
        }
    }

    public void releasePage(MemoryMappedPage page) throws IOException {
        int referenceCount = page.release();
        if (referenceCount <= 0) {
            page.close();
        }
    }

    private MemoryMappedPage createPage(int position) throws IOException {
        long offset = pageSize * position;
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, offset, pageSize);
        buffer.load();
        buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        return new MemoryMappedPage(buffer, offset);
    }

    public static class MemoryMappedPage implements Closeable {

        private final AtomicInteger referenceCount = new AtomicInteger();
        private final long offset;
        private MappedByteBuffer buffer;
        private boolean dirty = false;
        private boolean closed = false;

        private MemoryMappedPage(MappedByteBuffer buffer, long offset) {
            this.buffer = buffer;
            this.offset = offset;
        }

        private int getIndex(long offset) {
            return (int) (offset - this.offset);
        }

        public void putLong(long offset, long value) {
            buffer.putLong(getIndex(offset), value);
            dirty = true;
        }

        public long getLong(long offset) {
            return buffer.getLong(getIndex(offset));
        }

        public void putInt(long offset, int value) {
            buffer.putInt(getIndex(offset), value);
            dirty = true;
        }

        public int getInt(long offset) {
            return buffer.getInt(getIndex(offset));
        }

        public void putBytes(long offset, byte[] data, int start, int length) {
            int index = getIndex(offset);
            for (int i = 0; i < length; i++, index++, start++) {
                buffer.put(index, data[start]);
            }
            dirty = true;
        }

        public void getBytes(long offset, byte[] data, int start, int length) {
            int index = getIndex(offset);
            for (int i = 0; i < length; i++, index++, start++) {
                data[start] = buffer.get(index);
            }
        }

        public int acquire() {
            return referenceCount.incrementAndGet();
        }

        public int release() {
            return referenceCount.decrementAndGet();
        }

        public void flush() throws IOException {
            synchronized (this) {
                if (closed) return;
                if (dirty) {
                    buffer.force();
                    dirty = false;
                }
            }
        }

        @Override
        public synchronized void close() throws IOException {
            synchronized (this) {
                flush();
                if (closed) return;
                closed = true;
                MappedByteBufferCleaner.clean(buffer);
                buffer = null;
            }
        }
    }

    private static class MappedByteBufferCleaner {
        public static final boolean cleanSupported;
        private static final Method directCleaner;
        private static final Method directCleanerClean;

        static {
            Method directBufferCleanerX = null;
            Method directBufferCleanerCleanX = null;
            boolean v;
            try {
                directBufferCleanerX = Class.forName("java.nio.DirectByteBuffer").getMethod("cleaner");
                directBufferCleanerX.setAccessible(true);
                directBufferCleanerCleanX = Class.forName("sun.misc.Cleaner").getMethod("clean");
                directBufferCleanerCleanX.setAccessible(true);
                v = true;
            } catch (Exception e) {
                v = false;
            }
            cleanSupported = v;
            directCleaner = directBufferCleanerX;
            directCleanerClean = directBufferCleanerCleanX;
        }

        public static void clean(MappedByteBuffer buffer) {
            if (buffer == null) return;
            if (cleanSupported && buffer.isDirect()) {
                try {
                    Object cleaner = directCleaner.invoke(buffer);
                    directCleanerClean.invoke(cleaner);
                } catch (Exception e) {
                    // silently ignore exception
                }
            }
        }
    }
}
