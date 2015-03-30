package org.mitallast.queue.common.mmap.async;

import org.mitallast.queue.common.concurrent.futures.SmartFuture;
import org.mitallast.queue.common.mmap.MemoryMappedPage;

import java.io.Closeable;
import java.io.IOException;

public interface MemoryMappedPageCache extends Closeable {

    SmartFuture<MemoryMappedPage> acquire(long offset);

    void release(MemoryMappedPage page) throws IOException;

    void flush() throws IOException;
}
