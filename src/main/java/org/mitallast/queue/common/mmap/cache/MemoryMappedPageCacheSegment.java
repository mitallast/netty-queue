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
        pageMap = new ConcurrentHashMap<>(maxPages, 0.5f);
        pageLock = new MapReentrantLock(maxPages);
        gcLock = new ReentrantLock();
    }

    @Override
    public MemoryMappedPage acquire(long offset) throws IOException {
        MemoryMappedPage page = pageMap.get(offset);
        if (page != null) {
            assert page.acquire() > 1;
            page.setTimestamp(System.currentTimeMillis());
            return page;
        }
        pageLock.get(offset).lock();
        try {
            page = pageMap.get(offset);
            if (page == null) {
                page = loader.load(offset);
                assert page.acquire() == 1;
                page.setTimestamp(System.currentTimeMillis());
                assert pageMap.put(offset, page) == null;
            }
            assert page.acquire() > 1;
            page.setTimestamp(System.currentTimeMillis());
            return page;
        } finally {
            pageLock.get(offset).unlock();
        }
    }

    @Override
    public void release(MemoryMappedPage page) throws IOException {
        page.release();
        garbageCollect();
    }

    @Override
    public void flush() throws IOException {
        for (MemoryMappedPage page : pageMap.values()) {
            pageLock.get(page.getOffset()).lock();
            try {
                page.flush();
            } finally {
                pageLock.get(page.getOffset()).unlock();
            }
        }
    }

    @Override
    public void close() throws IOException {
        for (MemoryMappedPage page : pageMap.values()) {
            pageLock.get(page.getOffset()).lock();
            try {
                page.close();
            } finally {
                pageLock.get(page.getOffset()).unlock();
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
                    pageLock.get(page.getOffset()).lock();
                    try {
                        if (page.release() == 0) {
                            pageMap.remove(page.getOffset());
                            garbage.remove(i);
                            page.close();
                        }
                    } finally {
                        pageLock.get(page.getOffset()).unlock();
                    }
                }
            } finally {
                gcLock.unlock();
            }
        }
    }
}
