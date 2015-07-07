package org.mitallast.queue.raft.log;

import org.mitallast.queue.common.mmap.MemoryMappedFileBuffer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.BitSet;

/**
 * Entry structure:
 * <pre>
 *  {
 *      int offset
 *      int length
 *      long position
 *  }
 * </pre
 *
 * If segment is not full, last entry with offset==-1 marks as end
 */
public class SegmentIndex implements Closeable {
    private static final long MAX_POSITION = (long) Math.pow(2, 32) - 1;

    private static final long OFFSET_SIZE = 4;
    private static final long LENGTH_SIZE = 4;
    private static final long POSITION_SIZE = 8;

    public static final long ENTRY_SIZE = OFFSET_SIZE + LENGTH_SIZE + POSITION_SIZE;

    private final File file;
    private final MemoryMappedFileBuffer fileBuffer;
    private final MappedByteBuffer mappedByteBuffer;
    private final BitSet bits;
    private final int maxSize;
    private final int bitsSize;

    private int size = 0;
    private int firstOffset = -1;
    private int lastOffset = -1;

    public SegmentIndex(File file, int maxEntries) throws IOException {
        boolean needInit = file.length() > 0;

        this.file = file;
        this.maxSize = (int) (maxEntries * ENTRY_SIZE);
        this.fileBuffer = new MemoryMappedFileBuffer(file, maxSize);
        this.mappedByteBuffer = fileBuffer.mappedByteBuffer();
        this.bitsSize = (int) toPow2(maxEntries);
        this.bits = new BitSet(this.bitsSize);

        if (needInit) {
            init();
        }
    }

    private void init() throws IOException {
        for (int i = 0; i < maxSize; i += ENTRY_SIZE) {
            int offset = mappedByteBuffer.getInt(i);
            if (offset == -1) {
                break;
            }
            if (firstOffset == -1) {
                firstOffset = offset;
            }
            lastOffset = offset;
            bits.set(offset % bitsSize);
            size++;
        }
    }

    public File file() {
        return file;
    }

    public int firstOffset() {
        return firstOffset;
    }

    public int lastOffset() {
        return lastOffset;
    }

    public long lastPosition() throws IOException {
        return lastOffset >= 0
            ? position(lastOffset)
            : 0;
    }

    public int lastLength() throws IOException {
        return lastOffset >= 0
            ? length(lastOffset)
            : 0;
    }

    public long nextPosition() throws IOException {
        return lastPosition() + lastLength();
    }

    public int size() {
        return size;
    }

    public void index(int offset, long position, int length) throws IOException {
        if (lastOffset > -1 && offset <= lastOffset) {
            throw new IllegalArgumentException("offset cannot be less than or equal to the last offset in the index");
        }

        if (position > MAX_POSITION) {
            throw new IllegalArgumentException("position cannot be greater than " + MAX_POSITION);
        }
        // If the length is zero, that indicates that this is a skipped entry. We don't index skipped entries at all.
        if (length == 0) {
            return;
        }

        // seek to end of file
        long pos = size * ENTRY_SIZE;
        mappedByteBuffer.putInt((int) pos, offset);
        mappedByteBuffer.putInt((int) (pos + OFFSET_SIZE), length);
        mappedByteBuffer.putLong((int) (pos + OFFSET_SIZE + LENGTH_SIZE), position);
        if (pos + ENTRY_SIZE < maxSize) {
            mappedByteBuffer.putInt((int) (pos + ENTRY_SIZE), -1);
        }

        bits.set(offset % bitsSize);

        if (firstOffset == -1) {
            firstOffset = offset;
        }

        size++;
        lastOffset = offset;
    }

    public boolean contains(int offset) throws IOException {
        return position(offset) != -1;
    }

    public long position(int offset) throws IOException {
        if (!bits.get(offset % this.bitsSize)) {
            return -1;
        }

        long index = search(offset);
        if (index == -1) {
            return -1;
        }
        if (mappedByteBuffer.getInt((int) index) != offset) {
            throw new IOException("invalid offset");
        }
        return mappedByteBuffer.getLong((int) (index + OFFSET_SIZE + LENGTH_SIZE));
    }

    public int length(int offset) throws IOException {
        if (!bits.get(offset % this.bitsSize)) {
            return -1;
        }

        long index = search(offset);
        if (index == -1) {
            return -1;
        }
        if (mappedByteBuffer.getInt((int) index) != offset) {
            throw new IOException("invalid offset");
        }
        return mappedByteBuffer.getInt((int) (index + OFFSET_SIZE));
    }

    public long search(int offset) throws IOException {
        if (size == 0) {
            return -1;
        }

        int lo = 0;
        int hi = size - 1;

        while (lo < hi) {
            int mid = lo + (hi - lo) / 2;
            int i = mappedByteBuffer.getInt((int) (mid * ENTRY_SIZE));
            if (i == offset) {
                return mid * ENTRY_SIZE;
            } else if (lo == mid) {
                if (mappedByteBuffer.getInt((int) (hi * ENTRY_SIZE)) == offset) {
                    return hi * ENTRY_SIZE;
                }
                return -1;
            } else if (i < offset) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }

        if (mappedByteBuffer.getInt((int) (hi * ENTRY_SIZE)) == offset) {
            return hi * ENTRY_SIZE;
        }
        return -1;
    }

    public void truncate(int offset) throws IOException {
        if (offset == lastOffset)
            return;

        long index = search(offset + 1);

        if (index == -1)
            throw new IllegalStateException("unknown offset: " + offset);

        int lastOffset = lastOffset();
        for (int i = lastOffset; i > offset; i--) {
            if (position(i) != -1) {
                size--;
            }
        }
        fileBuffer.randomAccessFile().setLength(index);
        this.lastOffset = offset;
    }

    public void delete() throws IOException {
        close();
        assert file.delete();
    }

    public void flush() throws IOException {
        fileBuffer.flush();
    }

    public void close() throws IOException {
        fileBuffer.flush();
        fileBuffer.close();
    }

    private static long toPow2(long size) {
        if ((size & (size - 1)) == 0)
            return size;
        int i = 128;
        while (i < size) {
            i *= 2;
            if (i <= 0) return 1L << 62;
        }
        return i;
    }
}
