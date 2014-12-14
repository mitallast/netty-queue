package org.mitallast.queue.queue.service.translog.cache;

import org.mitallast.queue.queue.service.translog.MemoryMappedPage;

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
    private final ReentrantLock writeLock = new ReentrantLock();

    public MemoryMappedPageCacheSegment(Loader loader, int maxPages) {
        this.loader = loader;
        this.maxPages = maxPages;
        pageMap = new ConcurrentHashMap<>(maxPages, 0.5f);
    }

    @Override
    public MemoryMappedPage acquire(long offset) throws IOException {
        MemoryMappedPage page = pageMap.get(offset);
        if (page != null) {
            page.acquire();
            page.setTimestamp(System.currentTimeMillis());
            return page;
        }
        writeLock.lock();
        try {
            page = pageMap.get(offset);
            if (page == null) {
                page = loader.load(offset);
                page.acquire();
                page.setTimestamp(System.currentTimeMillis());
                assert pageMap.put(offset, page) == null;
            }
            page.acquire();
            page.setTimestamp(System.currentTimeMillis());
            return page;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void release(MemoryMappedPage page) throws IOException {
        writeLock.lock();
        try {
            internalRelease(page);
            garbageCollect();
        } finally {
            writeLock.unlock();
        }
    }

    private boolean internalRelease(MemoryMappedPage page) throws IOException {
        if (page.release() == 0) {
            pageMap.remove(page.getOffset());
            page.close();
            return true;
        }
        return false;
    }

    @Override
    public void flush() throws IOException {
        writeLock.lock();
        try {
            for (MemoryMappedPage page : pageMap.values()) {
                page.flush();
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        writeLock.lock();
        try {
            for (MemoryMappedPage page : pageMap.values()) {
                page.close();
            }
            pageMap.clear();
        } finally {
            writeLock.unlock();
        }
    }

    private void garbageCollect() throws IOException {
        if (pageMap.size() <= maxPages) {
            return;
        }
        if (pageMap.size() > maxPages) {
            garbage.addAll(pageMap.values());
            Collections.sort(garbage, reserveComparator);
            for (int i = garbage.size() - 1; i > maxPages; i--) {
                if (internalRelease(garbage.get(i))) {
                    garbage.remove(i);
                }
            }
        }
    }
}
