package org.mitallast.queue.queue.service.translog;

import java.io.Closeable;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class MemoryMappedPage implements Closeable {

    private final AtomicInteger referenceCount = new AtomicInteger();
    private final long offset;
    private MappedByteBuffer buffer;
    private boolean dirty = false;
    private boolean closed = false;

    public MemoryMappedPage(MappedByteBuffer buffer, long offset) {
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
    public void close() throws IOException {
        synchronized (this) {
            flush();
            if (closed) return;
            closed = true;
            MappedByteBufferCleaner.clean(buffer);
            buffer = null;
        }
    }
}