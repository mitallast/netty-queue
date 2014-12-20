package org.mitallast.queue.common.mmap.cache;

import com.google.common.primitives.Longs;
import org.mitallast.queue.common.mmap.MemoryMappedPage;

import java.io.IOException;

public class MemoryMappedPageCacheSegmented implements MemoryMappedPageCache {

    private final MemoryMappedPageCacheSegment[] segments;

    public MemoryMappedPageCacheSegmented(Loader loader, int maxPages, int segmentsCount) {
        segments = new MemoryMappedPageCacheSegment[segmentsCount];
        for (int i = 0; i < segmentsCount; i++) {
            segments[i] = new MemoryMappedPageCacheSegment(loader, maxPages);
        }
    }

    @Override
    public MemoryMappedPage acquire(long offset) throws IOException {
        return getSegment(offset).acquire(offset);
    }

    @Override
    public void release(MemoryMappedPage page) throws IOException {
        getSegment(page.getOffset()).release(page);
    }

    @Override
    public void flush() throws IOException {
        for (MemoryMappedPageCacheSegment segment : segments) {
            segment.flush();
        }
    }

    @Override
    public void close() throws IOException {
        for (MemoryMappedPageCacheSegment segment : segments) {
            segment.close();
        }
    }

    private MemoryMappedPageCacheSegment getSegment(long offset) {
        return segments[index(hash(offset))];
    }

    private long hash(long offset) {
        return Longs.hashCode(offset);
    }

    public int index(long hash) {
        return (int) (hash % segments.length);
    }
}
