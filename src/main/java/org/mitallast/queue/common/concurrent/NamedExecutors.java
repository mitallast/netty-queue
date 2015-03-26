package org.mitallast.queue.common.concurrent;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedExecutors {

    public static ExecutorService newFixedThreadPool(String name, int nThreads) {
        return new ThreadPoolExecutor(
            nThreads,
            nThreads,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new NamedThreadFactory(name)
        );
    }

    public static ThreadFactory newThreadFactory(String name) {
        return new NamedThreadFactory(name);
    }

    private static class NamedThreadFactory implements ThreadFactory {
        private final static AtomicInteger poolNumber = new AtomicInteger(1);
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);

        private NamedThreadFactory(String group) {
            this.group = new ThreadGroup(group + '[' + poolNumber.getAndIncrement() + ']');
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(
                group,
                r,
                group.getName() + '[' + threadNumber.getAndIncrement() + ']',
                0
            );
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }
}
