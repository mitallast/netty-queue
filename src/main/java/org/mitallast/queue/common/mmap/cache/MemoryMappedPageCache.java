package org.mitallast.queue.common.mmap.cache;

import org.mitallast.queue.common.mmap.MemoryMappedPage;

import java.io.Closeable;
import java.io.IOException;

public interface MemoryMappedPageCache extends Closeable {

    MemoryMappedPage acquire(long offset) throws IOException;

    void release(MemoryMappedPage page) throws IOException;

    void flush() throws IOException;

    public static interface Loader {
        public MemoryMappedPage load(long offset) throws IOException;
    }
}
