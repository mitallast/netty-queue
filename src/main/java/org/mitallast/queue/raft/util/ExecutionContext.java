package org.mitallast.queue.raft.util;

import com.google.inject.Inject;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.concurrent.NamedExecutors;
import org.mitallast.queue.common.settings.Settings;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;

public class ExecutionContext extends AbstractLifecycleComponent {
    private final ScheduledExecutorService executorService;
    private final Executor executor;
    private final Thread executorThread;

    @Inject
    public ExecutionContext(Settings settings) throws ExecutionException, InterruptedException {
        super(settings);
        executorService = NamedExecutors.newScheduledSingleThreadPool("raft");
        executorThread = executorService.submit(Thread::currentThread).get();
        executor = command -> executorService.execute(() -> {
            try {
                command.run();
            } catch (Throwable e) {
                logger.error("unexpected error", e);
                throw new RuntimeException(e);
            }
        });
    }

    public Executor executor() {
        return executor;
    }

    public void execute(Runnable runnable) {
        executorService.execute(() -> {
            try {
                runnable.run();
            } catch (Throwable e) {
                logger.error("unexpected error", e);
                throw new RuntimeException(e);
            }
        });
    }

    public <T> Future<T> submit(Callable<T> callable) {
        return executorService.submit(() -> {
            try {
                return callable.call();
            } catch (Throwable e) {
                logger.error("unexpected error", e);
                throw new RuntimeException(e);
            }
        });
    }

    public Future submit(Runnable runnable) {
        return executorService.submit(() -> {
            try {
                runnable.run();
            } catch (Throwable e) {
                logger.error("unexpected error", e);
                throw new RuntimeException(e);
            }
        });
    }

    public ScheduledFuture<?> schedule(Runnable runnable, long delay, TimeUnit unit) {
        return executorService.schedule(() -> {
            try {
                runnable.run();
            } catch (Throwable e) {
                logger.error("unexpected error", e);
                throw new RuntimeException(e);
            }
        }, delay, unit);
    }

    public ScheduledFuture<?> scheduleAtFixedRate(Runnable runnable, long initialDelay, long period, TimeUnit unit) {
        return executorService.scheduleAtFixedRate(() -> {
            try {
                runnable.run();
            } catch (Throwable e) {
                logger.error("unexpected error", e);
                throw new RuntimeException(e);
            }
        }, initialDelay, period, unit);
    }

    public void checkThread() {
        if (!Thread.currentThread().equals(executorThread)) {
            IllegalStateException error = new IllegalStateException("Does not raft thread");
            logger.warn("does not raft thread", error);
            throw error;
        }
    }

    @Override
    protected void doStart() throws IOException {
    }

    @Override
    protected void doStop() throws IOException {
    }

    @Override
    protected void doClose() throws IOException {
        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.warn("error close executor", e);
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
        List<Runnable> failed = executorService.shutdownNow();
        if (!failed.isEmpty()) {
            logger.warn("failed to execute {}", failed);
        }
    }
}
