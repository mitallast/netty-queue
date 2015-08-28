package org.mitallast.queue.log;

import io.netty.buffer.ByteBuf;
import org.mitallast.queue.common.collection.HashFunctions;
import org.mitallast.queue.common.mmap.MemoryMappedFileBuffer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.BitSet;


/**
 * Entry structure:
 * <pre>
 * {
 *     long offset;
 *     long position;
 *     int length;
 *     int status;
 * }
 * </pre>
 */
public class SegmentIndex implements Closeable {

    private static final long MAX_POSITION = (long) Math.pow(2, 32) - 1;

    private static final int OFFSET_SIZE = Long.BYTES;
    private static final int POSITION_SIZE = Long.BYTES;
    private static final int LENGTH_SIZE = Integer.BYTES;
    private static final int STATUS_SIZE = Integer.BYTES;

    public static final int ENTRY_SIZE = OFFSET_SIZE + POSITION_SIZE + LENGTH_SIZE + STATUS_SIZE;

    private final File file;
    private final BitSet bits;
    private final int maxSize;
    private final int bitsSize;
    private final MemoryMappedFileBuffer fileBuffer;

    private ByteBuf buffer;
    private int size = 0;

    private long firstOffset = -1;
    private long lastOffset = -1;

    public SegmentIndex(File file, int maxEntries) throws IOException {
        boolean needInit = file.length() > 0;

        this.file = file;
        this.maxSize = maxEntries * ENTRY_SIZE;
        this.fileBuffer = new MemoryMappedFileBuffer(file, maxSize);
        this.buffer = fileBuffer.buffer();
        this.bitsSize = (int) HashFunctions.toPow2(maxEntries);
        this.bits = new BitSet(this.bitsSize);

        if (needInit) {
            init();
        }
    }

    private void init() throws IOException {
        for (int i = 0; i < maxSize; i += ENTRY_SIZE) {
            long offset = buffer.getLong(i);
            if (offset == -1) {
                break;
            }
            if (firstOffset == -1) {
                firstOffset = offset;
            }
            lastOffset = offset;
            bits.set((int) (offset % bitsSize));
            size++;
        }
    }

    public File file() {
        return file;
    }

    public long firstOffset() {
        return firstOffset;
    }

    public long lastOffset() {
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

    public void index(long offset, long position, int length, MessageStatus status) throws IOException {
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
        int pos = size * ENTRY_SIZE;
        buffer.setLong(pos, offset);
        buffer.setLong(pos + OFFSET_SIZE, position);
        buffer.setInt(pos + OFFSET_SIZE + POSITION_SIZE, length);
        buffer.setInt(pos + OFFSET_SIZE + POSITION_SIZE + LENGTH_SIZE, status.ordinal());
        if (pos + ENTRY_SIZE < maxSize) {
            buffer.setLong(pos + ENTRY_SIZE, -1);
        }

        bits.set((int) (offset % bitsSize));

        if (firstOffset == -1) {
            firstOffset = offset;
        }

        size++;
        lastOffset = offset;
    }

    public MessageMeta peek() throws IOException {
        for (int index = 0; index < maxSize; index += ENTRY_SIZE) {
            long offset = buffer.getLong(index);
            if (offset == -1) {
                break;
            }
            int status = buffer.getInt(index + OFFSET_SIZE + POSITION_SIZE + LENGTH_SIZE);
            if (status == MessageStatus.QUEUED.ordinal()) {
                return new MessageMeta(
                    offset,
                    buffer.getLong(index + OFFSET_SIZE),
                    buffer.getInt(index + OFFSET_SIZE + POSITION_SIZE),
                    MessageStatus.QUEUED
                );
            }
        }
        return null;
    }

    public MessageMeta lockAndPop() throws IOException {
        for (int index = 0; index < maxSize; index += ENTRY_SIZE) {
            long offset = buffer.getLong(index);
            if (offset == -1) {
                break;
            }
            int status = buffer.getInt(index + OFFSET_SIZE + POSITION_SIZE + LENGTH_SIZE);
            if (status == MessageStatus.QUEUED.ordinal()) {
                buffer.setInt(index + OFFSET_SIZE + POSITION_SIZE + LENGTH_SIZE, MessageStatus.LOCKED.ordinal());
                return new MessageMeta(
                    offset,
                    buffer.getLong(index + OFFSET_SIZE),
                    buffer.getInt(index + OFFSET_SIZE + POSITION_SIZE),
                    MessageStatus.LOCKED
                );
            }
        }
        return null;
    }

    public MessageMeta lock(long offset) throws IOException {
        int index = search(offset);
        if (buffer.getLong(index) != offset) {
            return null;
        }
        int status = buffer.getInt(index + OFFSET_SIZE + POSITION_SIZE + LENGTH_SIZE);
        if (status == MessageStatus.QUEUED.ordinal()) {
            buffer.setInt(index + OFFSET_SIZE + POSITION_SIZE + LENGTH_SIZE, MessageStatus.LOCKED.ordinal());
            return new MessageMeta(
                offset,
                buffer.getLong(index + OFFSET_SIZE),
                buffer.getInt(index + OFFSET_SIZE + POSITION_SIZE),
                MessageStatus.LOCKED
            );
        }
        return null;
    }

    public MessageMeta unlockAndDelete(long offset) throws IOException {
        int index = search(offset);
        if (buffer.getLong(index) != offset) {
            return null;
        }
        int status = buffer.getInt(index + OFFSET_SIZE + POSITION_SIZE + LENGTH_SIZE);
        if (status == MessageStatus.LOCKED.ordinal()) {
            buffer.setInt(index + OFFSET_SIZE + POSITION_SIZE + LENGTH_SIZE, MessageStatus.DELETED.ordinal());
            return new MessageMeta(
                offset,
                buffer.getLong(index + OFFSET_SIZE),
                buffer.getInt(index + OFFSET_SIZE + POSITION_SIZE),
                MessageStatus.LOCKED
            );
        }
        return null;
    }

    public MessageMeta unlockAndQueue(long offset) throws IOException {
        int index = search(offset);
        if (buffer.getLong(index) != offset) {
            return null;
        }
        int status = buffer.getInt(index + OFFSET_SIZE + POSITION_SIZE + LENGTH_SIZE);
        if (status == MessageStatus.LOCKED.ordinal()) {
            buffer.setInt(index + OFFSET_SIZE + POSITION_SIZE + LENGTH_SIZE, MessageStatus.QUEUED.ordinal());
            return new MessageMeta(
                offset,
                buffer.getLong(index + OFFSET_SIZE),
                buffer.getInt(index + OFFSET_SIZE + POSITION_SIZE),
                MessageStatus.LOCKED
            );
        }
        return null;
    }

    public boolean contains(long offset) throws IOException {
        return search(offset) != -1;
    }

    public MessageMeta meta(long offset) throws IOException {
        int index = search(offset);
        if (index == -1) {
            return null;
        }
        return new MessageMeta(
            offset,
            buffer.getLong(index + OFFSET_SIZE),
            buffer.getInt(index + OFFSET_SIZE + POSITION_SIZE),
            buffer.getInt(index + OFFSET_SIZE + POSITION_SIZE + LENGTH_SIZE)
        );
    }

    public long position(long offset) throws IOException {
        if (!bits.get((int) (offset % this.bitsSize))) {
            return -1;
        }

        int index = search(offset);
        if (index == -1) {
            return -1;
        }
        return buffer.getLong(index + OFFSET_SIZE);
    }

    public int length(long offset) throws IOException {
        if (!bits.get((int) (offset % this.bitsSize))) {
            return -1;
        }

        int index = search(offset);
        if (index == -1) {
            return -1;
        }
        return buffer.getInt(index + OFFSET_SIZE + POSITION_SIZE);
    }

    public boolean isGarbage() throws IOException {
        for (int index = 0; index < maxSize; index += ENTRY_SIZE) {
            long offset = buffer.getLong(index);
            if (offset == -1) {
                return false;
            }
            int status = buffer.getInt(index + OFFSET_SIZE + POSITION_SIZE + LENGTH_SIZE);
            if (status != MessageStatus.DELETED.ordinal()) {
                return false;
            }
        }
        return true;
    }

    public int search(long offset) throws IOException {
        if (size == 0) {
            return -1;
        }

        int lo = 0;
        int hi = size - 1;

        while (lo < hi) {
            int mid = lo + (hi - lo) / 2;
            long i = buffer.getLong(mid * ENTRY_SIZE);
            if (i == offset) {
                return mid * ENTRY_SIZE;
            } else if (lo == mid) {
                if (buffer.getLong(hi * ENTRY_SIZE) == offset) {
                    return hi * ENTRY_SIZE;
                }
                return -1;
            } else if (i < offset) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }

        if (buffer.getLong(hi * ENTRY_SIZE) == offset) {
            return hi * ENTRY_SIZE;
        }
        return -1;
    }

    public void truncate(final long offset) throws IOException {
        if (offset == lastOffset)
            return;

        int index = search(offset + 1);
        if (index == -1)
            throw new IllegalStateException("unknown offset: " + offset);

        for (long i = offset + 1; i <= lastOffset; i++) {
            int i_index = search(i);
            if (i_index != -1) {
                long i_position = buffer.getLong(i_index + OFFSET_SIZE);
                if (i_position != -1) {
                    size--;
                }
                // clear
                buffer.setLong(i_index, -1);
            }
        }
        this.lastOffset = offset;
    }

    public void delete() throws IOException {
        close();
        assert file.delete();
    }

    public void flush() throws IOException {
        fileBuffer.flush();
    }

    @Override
    public void close() throws IOException {
        fileBuffer.close();
        buffer = null;
    }
}
