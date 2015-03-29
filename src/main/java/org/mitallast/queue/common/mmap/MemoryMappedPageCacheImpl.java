package org.mitallast.queue.common.mmap;

import org.mitallast.queue.common.concurrent.MapReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class MemoryMappedPageCacheImpl implements MemoryMappedPageCache {

    private final static Logger logger = LoggerFactory.getLogger(MemoryMappedPageCacheImpl.class);

    private final static Comparator<MemoryMappedPage> comparator =
        (o1, o2) -> (int) (o1.getTimestamp() - o2.getTimestamp());
    private final MemoryMappedPageCacheLoader loader;
    private final int maxPages;
    private final ConcurrentHashMap<Long, MemoryMappedPage> pageMap;
    private final ArrayList<MemoryMappedPage> garbage = new ArrayList<>();
    private final MapReentrantLock pageLock;
    private final ReentrantLock gcLock;
    private final AtomicInteger pagesCount;

    public MemoryMappedPageCacheImpl(MemoryMappedPageCacheLoader loader, int maxPages) {
        this.loader = loader;
        this.maxPages = maxPages;
        pageMap = new ConcurrentHashMap<>();
        pageLock = new MapReentrantLock(maxPages);
        gcLock = new ReentrantLock();
        pagesCount = new AtomicInteger();
    }

    @Override
    public MemoryMappedPage acquire(final long offset) throws IOException {
        MemoryMappedPage page;
        page = pageMap.get(offset);
        if (page != null && page.acquire()) {
            page.updateTimestamp(System.currentTimeMillis());
            return page;
        }
        final ReentrantLock lock = pageLock.get(offset);
        lock.lock();
        try {
            page = pageMap.get(offset);
            if (page != null && page.acquire()) {
                page.updateTimestamp(System.currentTimeMillis());
                return page;
            }
            page = loader.load(offset);
            assert page.acquire();
            page.updateTimestamp(System.currentTimeMillis());
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
            if (page.acquire()) {
                try {
                    page.flush();
                } finally {
                    page.release();
                }
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
        if (pagesCount.get() > maxPages * 2 && gcLock.tryLock()) {
            try {
                logger.info("gc pages: {} max {}", pagesCount.get(), maxPages);
                garbage.clear();
                garbage.addAll(pageMap.values());
                garbage.forEach(MemoryMappedPage::lockTimestamp);
                Collections.sort(garbage, comparator);
                garbage.forEach(MemoryMappedPage::unlockTimestamp);
                for (int i = garbage.size() - 1; i > 0 && garbage.size() > maxPages; i--) {
                    MemoryMappedPage page = garbage.get(i);
                    if (page.garbage()) {
                        pageMap.remove(page.getOffset(), page);
                        garbage.remove(i);
                        pagesCount.decrementAndGet();
                        page.close();
                    }
                }
                logger.info("after gc pages: {}", pagesCount.get());
            } finally {
                gcLock.unlock();
            }
        }
    }
}
