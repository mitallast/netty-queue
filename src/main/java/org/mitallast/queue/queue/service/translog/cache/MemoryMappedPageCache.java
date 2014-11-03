package org.mitallast.queue.queue.service.translog.cache;

import org.mitallast.queue.queue.service.translog.MemoryMappedPage;

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
