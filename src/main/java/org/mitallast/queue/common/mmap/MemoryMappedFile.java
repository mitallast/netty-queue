package org.mitallast.queue.common.mmap;

import io.netty.buffer.ByteBuf;
import org.mitallast.queue.common.mmap.cache.MemoryMappedPageCache;
import org.mitallast.queue.common.mmap.cache.MemoryMappedPageCacheSegment;
import org.mitallast.queue.common.mmap.cache.MemoryMappedPageCacheSegmented;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MemoryMappedFile implements MemoryMappedPageCacheSegment.Loader, Closeable {
    public final static int DEFAULT_PAGE_SIZE = 1048576;
    public final static int DEFAULT_MAX_PAGES = 64;
    private final File file;
    private final RandomAccessFile randomAccessFile;
    private final int pageSize;
    private final FileChannel channel;
    private final MemoryMappedPageCache pageCache;

    public MemoryMappedFile(File file) throws IOException {
        this(file, DEFAULT_PAGE_SIZE, DEFAULT_MAX_PAGES);
    }

    public MemoryMappedFile(File file, int pageSize, int maxPages) throws IOException {
        this.file = file;
        this.randomAccessFile = new RandomAccessFile(file, "rw");
        this.pageSize = pageSize;
        channel = randomAccessFile.getChannel();
        pageCache = new MemoryMappedPageCacheSegmented(this, maxPages, 10);
    }

    @Override
    public void close() throws IOException {
        pageCache.close();
        channel.close();
        randomAccessFile.close();
    }

    public void delete() throws IOException {
        close();
        if (file.exists() && !file.delete()) {
            throw new IOException("Error delete file " + file);
        }
    }

    public void putLong(long offset, long value) throws IOException {
        MemoryMappedPage page = getPage(offset);
        long pageMax = page.getOffset() + pageSize;
        try {
            if (offset + 8 < pageMax) {
                page.putLong(offset, value);
            } else {
                page.putInt(offset, (int) (value >>> 32));
            }
        } finally {
            releasePage(page);
        }
        if (offset + 8 >= pageMax) {
            putInt(offset + 4, (int) value);
        }
    }

    public long getLong(long offset) throws IOException {
        MemoryMappedPage page = getPage(offset);
        long pageMax = page.getOffset() + pageSize;
        long value = 0;
        try {
            if (offset + 8 < pageMax) {
                value = page.getLong(offset);
            } else {
                value = page.getInt(offset) & 0xffffffffL;
                value = value << 32;
            }
        } finally {
            releasePage(page);
        }
        if (offset + 8 >= pageMax) {
            value |= getInt(offset + 4) & 0xffffffffL;
        }
        return value;
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

    public void getBytes(long offset, ByteBuf buffer, int length) throws IOException {
        long position = offset;
        long end = offset + length;
        int dataPosition = 0;
        while (position < end) {
            int max = pageSize - (int) (position % pageSize);
            max = Math.min(max, length - dataPosition);
            MemoryMappedPage page = getPage(position);
            try {
                page.getBytes(position, buffer, max);
            } finally {
                releasePage(page);
            }
            dataPosition += max;
            position += max;
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

    public void putBytes(long offset, ByteBuf byteBuf) throws IOException {
        putBytes(offset, byteBuf, byteBuf.readableBytes());
    }

    public void putBytes(long offset, ByteBuf byteBuf, int length) throws IOException {
        long position = offset;
        long end = offset + length;
        int dataPosition = 0;
        while (position < end) {
            int max = pageSize - (int) (position % pageSize);
            max = Math.min(max, length - dataPosition);
            MemoryMappedPage page = getPage(position);
            try {
                page.putBytes(position, byteBuf, max);
            } finally {
                releasePage(page);
            }
            dataPosition += max;
            position += max;
        }
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
        pageCache.flush();
    }

    public MemoryMappedPage getPage(long offset) throws IOException {
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
