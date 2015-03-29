package org.mitallast.queue.common.mmap;

import java.io.IOException;

public interface MemoryMappedPageCacheLoader {
    public MemoryMappedPage load(long offset) throws IOException;
}
