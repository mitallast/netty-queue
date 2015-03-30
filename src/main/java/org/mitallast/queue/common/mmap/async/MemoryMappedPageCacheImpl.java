package org.mitallast.queue.common.mmap.async;

import org.mitallast.queue.common.concurrent.MapReentrantLock;
import org.mitallast.queue.common.concurrent.futures.Futures;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;
import org.mitallast.queue.common.mmap.MemoryMappedPage;
import org.mitallast.queue.common.mmap.MemoryMappedPageCacheLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <code>
 * MemoryMappedPageCache cache = ...;
 * cache.acquire(1024).on(result -> {
 * MemoryMappedPage page = result.getOrNull();
 * ...
 * cache.release(page);
 * });
 * <code>
 */
public class MemoryMappedPageCacheImpl implements MemoryMappedPageCache {

    private final static Logger logger = LoggerFactory.getLogger(MemoryMappedPageCacheImpl.class);

    private final static Comparator<SmartFuture<MemoryMappedPage>> comparator =
        (o1, o2) -> (int) (o1.getOrNull().getTimestamp() - o2.getOrNull().getTimestamp());

    private final MemoryMappedPageCacheLoader loader;
    private final int maxPages;
    private final ConcurrentHashMap<Long, SmartFuture<MemoryMappedPage>> pageFutureMap;
    private final ConcurrentHashMap<Long, AtomicInteger> pageRefMap;
    private final ArrayList<SmartFuture<MemoryMappedPage>> garbage;
    private final MapReentrantLock pageLock;
    private final ReentrantLock gcLock;
    private final AtomicInteger pagesCount;

    public MemoryMappedPageCacheImpl(MemoryMappedPageCacheLoader loader, int maxPages) {
        this.maxPages = maxPages;
        this.loader = loader;
        this.pageFutureMap = new ConcurrentHashMap<>(maxPages);
        this.pageRefMap = new ConcurrentHashMap<>(maxPages);
        this.garbage = new ArrayList<>(maxPages * 2);
        this.pageLock = new MapReentrantLock(maxPages * 2);
        this.gcLock = new ReentrantLock();
        this.pagesCount = new AtomicInteger();
    }

    @Override
    public SmartFuture<MemoryMappedPage> acquire(long offset) {
        SmartFuture<MemoryMappedPage> current, newFuture = null;
        while (true) {
            current = pageFutureMap.get(offset);
            if (current != null) {
                if (acquireRef(offset)) {
                    return current;
                } else {
                    if (newFuture == null) {
                        newFuture = Futures.future();
                    }
                    if (pageFutureMap.replace(offset, current, newFuture)) {
                        loadPage(newFuture, offset);
                    }
                }
            } else {
                if (newFuture == null) {
                    newFuture = Futures.future();
                }
                if (pageFutureMap.putIfAbsent(offset, newFuture) == null) {
                    loadPage(newFuture, offset);
                }
            }
        }
    }

    private void loadPage(SmartFuture<MemoryMappedPage> future, long offset) {
        final ReentrantLock lock = pageLock.get(offset);
        lock.lock();
        try {
            if (pageFutureMap.get(offset) == future) {
                MemoryMappedPage page = loader.load(offset);
                page.updateTimestamp(System.currentTimeMillis());
                pagesCount.incrementAndGet();
                future.invoke(page);
            }
        } catch (IOException e) {
            future.invokeException(e);
        } finally {
            lock.unlock();
        }
    }

    private boolean acquireRef(long offset) {
        AtomicInteger referenceCount = pageRefMap.computeIfAbsent(offset, offset_ -> new AtomicInteger());
        while (true) {
            int current = referenceCount.get();
            if (current >= 0) {
                if (referenceCount.compareAndSet(current, current + 1)) {
                    return true;
                }
            } else {
                return false;
            }
        }
    }

    private boolean garbageRef(long offset) {
        AtomicInteger referenceCount = pageRefMap.get(offset);
        while (true) {
            int current = referenceCount.get();
            if (current == 0) {
                if (referenceCount.compareAndSet(current, current - 1)) {
                    return true;
                }
            } else {
                return false;
            }
        }
    }

    @Override
    public void release(MemoryMappedPage page) throws IOException {
        pageRefMap.get(page.getOffset()).decrementAndGet();
//        garbageCollect();
    }

    @Override
    public void flush() throws IOException {
        try {
            for (SmartFuture<MemoryMappedPage> future : pageFutureMap.values()) {
                MemoryMappedPage page = future.get();
                final ReentrantLock lock = pageLock.get(page.getOffset());
                lock.lock();
                try {
                    page.flush();
                } finally {
                    lock.unlock();
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            for (SmartFuture<MemoryMappedPage> future : pageFutureMap.values()) {
                MemoryMappedPage page = future.get();
                final ReentrantLock lock = pageLock.get(page.getOffset());
                lock.lock();
                try {
                    page.close();
                } finally {
                    lock.unlock();
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException(e);
        }
    }

    private void garbageCollect() throws IOException {
        if (pagesCount.get() > maxPages * 2 && gcLock.tryLock()) {
            try {
                logger.info("gc pages: {} max {}", pagesCount.get(), maxPages);
                garbage.clear();
                pageFutureMap.values().stream()
                    .filter(SmartFuture::isValuableDone)
                    .forEach(garbage::add);

                garbage.forEach(future -> future.getOrNull().lockTimestamp());
                Collections.sort(garbage, comparator);
                garbage.forEach(future -> future.getOrNull().unlockTimestamp());

                for (int i = garbage.size() - 1; i > 0 && garbage.size() > maxPages; i--) {
                    SmartFuture<MemoryMappedPage> future = garbage.get(i);
                    MemoryMappedPage page = future.getOrNull();
                    if (garbageRef(page.getOffset())) {
                        pagesCount.decrementAndGet();
                        pageFutureMap.remove(page.getOffset(), future);
                        page.close();
                        garbage.remove(i);
                    }
                }
                logger.info("after gc pages: {}", pagesCount.get());
            } finally {
                gcLock.unlock();
            }
        }
    }
}
