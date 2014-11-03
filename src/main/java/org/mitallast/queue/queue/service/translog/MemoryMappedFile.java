package org.mitallast.queue.queue.service.translog;

import org.mitallast.queue.queue.service.translog.cache.MemoryMappedPageCache;
import org.mitallast.queue.queue.service.translog.cache.MemoryMappedPageCacheSegment;
import org.mitallast.queue.queue.service.translog.cache.MemoryMappedPageCacheSegmented;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MemoryMappedFile implements MemoryMappedPageCacheSegment.Loader, Closeable {
    private final RandomAccessFile randomAccessFile;
    private final int pageSize;
    private final FileChannel channel;

    private final MemoryMappedPageCache pageCache;

    public MemoryMappedFile(RandomAccessFile randomAccessFile, int pageSize, int maxPages) throws IOException {
        this.randomAccessFile = randomAccessFile;
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
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, offset, pageSize);
        buffer.load();
        buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        return new MemoryMappedPage(buffer, offset);
    }
}
