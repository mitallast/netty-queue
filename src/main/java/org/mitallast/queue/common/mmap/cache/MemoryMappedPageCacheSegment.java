package org.mitallast.queue.common.mmap.cache;

import org.mitallast.queue.common.concurrent.MapReentrantLock;
import org.mitallast.queue.common.mmap.MemoryMappedPage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class MemoryMappedPageCacheSegment implements MemoryMappedPageCache {

    private final static Comparator<MemoryMappedPage> reserveComparator = new Comparator<MemoryMappedPage>() {
        @Override
        public int compare(MemoryMappedPage o1, MemoryMappedPage o2) {
            return (int) (o2.getTimestamp() - o1.getTimestamp());
        }
    };
    private final Loader loader;
    private final int maxPages;
    private final Map<Long, MemoryMappedPage> pageMap;
    private final ArrayList<MemoryMappedPage> garbage = new ArrayList<>();
    private final MapReentrantLock pageLock;
    private final ReentrantLock gcLock;

    public MemoryMappedPageCacheSegment(Loader loader, int maxPages) {
        this.loader = loader;
        this.maxPages = maxPages;
        pageMap = new ConcurrentHashMap<>();
        pageLock = new MapReentrantLock(maxPages);
        gcLock = new ReentrantLock();
    }

    @Override
    public MemoryMappedPage acquire(final long offset) throws IOException {
        MemoryMappedPage page;
        page = pageMap.get(offset);
        if (page != null) {
            assert page.acquire() > 1;
            page.setTimestamp(System.currentTimeMillis());
            return page;
        }
        final ReentrantLock lock = pageLock.get(offset);
        lock.lock();
        try {
            page = pageMap.get(offset);
            if (page != null) {
                assert page.acquire() > 1;
                page.setTimestamp(System.currentTimeMillis());
                return page;
            }
            page = loader.load(offset);
            assert page.acquire() == 1; // allocation
            assert page.acquire() == 2; // acquire
            page.setTimestamp(System.currentTimeMillis());
            assert pageMap.put(offset, page) == null;
            return page;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void release(MemoryMappedPage page) throws IOException {
        page.release();
        garbageCollect();
    }

    @Override
    public synchronized void flush() throws IOException {
        for (MemoryMappedPage page : pageMap.values()) {
            final ReentrantLock lock = pageLock.get(page.getOffset());
            lock.lock();
            try {
                page.flush();
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        for (MemoryMappedPage page : pageMap.values()) {
            final ReentrantLock lock = pageLock.get(page.getOffset());
            lock.lock();
            try {
                page.close();
            } finally {
                lock.unlock();
            }
        }
    }

    private void garbageCollect() throws IOException {
        if (pageMap.size() > maxPages
                && !gcLock.isLocked()
                && gcLock.tryLock()) {
            try {
                garbage.clear();
                garbage.addAll(pageMap.values());
                Collections.sort(garbage, reserveComparator);
                for (int i = garbage.size() - 1; i > 0 && garbage.size() > maxPages; i--) {
                    MemoryMappedPage page = garbage.get(i);
                    final ReentrantLock lock = pageLock.get(page.getOffset());
                    lock.lock();
                    try {
                        if (page.release() == 0) {
                            pageMap.remove(page.getOffset());
                            garbage.remove(i);
                            page.close();
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            } finally {
                gcLock.unlock();
            }
        }
    }
}
