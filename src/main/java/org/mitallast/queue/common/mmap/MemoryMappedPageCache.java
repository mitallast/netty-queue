package org.mitallast.queue.common.mmap;

import java.io.Closeable;
import java.io.IOException;

public interface MemoryMappedPageCache extends Closeable {

    MemoryMappedPage acquire(long offset) throws IOException;

    void release(MemoryMappedPage page) throws IOException;

    void flush() throws IOException;

}
