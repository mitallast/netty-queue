package org.mitallast.queue.common.mmap.cache;

import org.mitallast.queue.common.concurrent.MapReentrantLock;
import org.mitallast.queue.common.mmap.MemoryMappedPage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemoryMappedPageCacheSegment implements MemoryMappedPageCache {

    private final static Comparator<MemoryMappedPage> reserveComparator =
        (o1, o2) -> (int) (o2.getTimestamp() - o1.getTimestamp());
    private final Loader loader;
    private final int maxPages;
    private final ConcurrentHashMap<Long, MemoryMappedPage> pageMap;
    private final ArrayList<MemoryMappedPage> garbage = new ArrayList<>();
    private final MapReentrantLock pageLock;
    private final ReentrantReadWriteLock gcLock;
    private final AtomicInteger pagesCount;

    public MemoryMappedPageCacheSegment(Loader loader, int maxPages) {
        this.loader = loader;
        this.maxPages = maxPages;
        pageMap = new ConcurrentHashMap<>();
        pageLock = new MapReentrantLock(maxPages);
        gcLock = new ReentrantReadWriteLock();
        pagesCount = new AtomicInteger();
    }

    @Override
    public MemoryMappedPage acquire(final long offset) throws IOException {
        MemoryMappedPage page;
        page = pageMap.get(offset);
        if (page != null) {
            while (true) {
                int rc = page.getReferenceCount();
                if (rc >= 1) {
                    if (page.setReferenceCount(rc, rc + 1)) {
                        if (gcLock.readLock().tryLock()) {
                            try {
                                page.setTimestamp(System.currentTimeMillis());
                            } finally {
                                gcLock.readLock().unlock();
                            }
                        }
                        return page;
                    }
                } else {
                    break;
                }
            }
        }
        final ReentrantLock lock = pageLock.get(offset);
        lock.lock();
        try {
            page = pageMap.get(offset);
            if (page != null) {
                while (true) {
                    int rc = page.getReferenceCount();
                    if (rc >= 1) {
                        if (page.setReferenceCount(rc, rc + 1)) {
                            if (gcLock.readLock().tryLock()) {
                                try {
                                    page.setTimestamp(System.currentTimeMillis());
                                } finally {
                                    gcLock.readLock().unlock();
                                }
                            }
                            return page;
                        } else {
                            break;
                        }
                    }
                }
            }
            page = loader.load(offset);
            assert page.acquire() == 1; // allocation
            assert page.acquire() == 2; // acquire
            page.setTimestamp(System.currentTimeMillis());
            pageMap.put(offset, page);
            pagesCount.incrementAndGet();
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
        if (pagesCount.get() > maxPages && gcLock.writeLock().tryLock()) {
            try {
                garbage.clear();
                garbage.addAll(pageMap.values());
                Collections.sort(garbage, reserveComparator);
                for (int i = garbage.size() - 1; i > 0 && garbage.size() > maxPages; i--) {
                    MemoryMappedPage page = garbage.get(i);
                    final ReentrantLock lock = pageLock.get(page.getOffset());
                    lock.lock();
                    try {
                        while (true) {
                            int rc = page.getReferenceCount();
                            if (rc == 1) {
                                if (page.setReferenceCount(rc, rc - 1)) {
                                    pageMap.remove(page.getOffset(), page);
                                    garbage.remove(i);
                                    pagesCount.decrementAndGet();
                                    page.close();
                                    break;
                                }
                            } else {
                                break;
                            }
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            } finally {
                gcLock.writeLock().unlock();
            }
        }
    }
}
