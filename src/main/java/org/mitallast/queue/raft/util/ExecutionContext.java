package org.mitallast.queue.raft.util;

import com.google.inject.Inject;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.concurrent.NamedExecutors;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.unit.TimeValue;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;

public class ExecutionContext extends AbstractLifecycleComponent {
    private final ScheduledExecutorService executorService;
    private final Thread executorThread;
    private final long errorThreshold;
    private final long warningThreshold;
    private final ConcurrentMap<String, TaskStatistics> taskStatistics;

    @Inject
    public ExecutionContext(Settings settings) throws ExecutionException, InterruptedException {
        super(settings);
        executorService = NamedExecutors.newScheduledSingleThreadPool("raft");
        executorThread = executorService.submit(Thread::currentThread).get();
        errorThreshold = componentSettings.getAsTime("error_threshold", TimeValue.timeValueMillis(200)).millis();
        warningThreshold = componentSettings.getAsTime("warning_threshold", TimeValue.timeValueMillis(100)).millis();
        taskStatistics = new ConcurrentHashMap<>();
    }

    public Executor executor(String context) {
        return runnable -> executorService.execute(new Task(context, runnable));
    }

    public void execute(String context, Runnable runnable) {
        executorService.execute(new Task(context, runnable));
    }

    public Future submit(String context, Runnable runnable) {
        return executorService.submit(new Task(context, runnable));
    }

    public ScheduledFuture<?> schedule(String context, Runnable runnable, long delay, TimeUnit unit) {
        return executorService.schedule(new Task(context, runnable), delay, unit);
    }

    public ScheduledFuture<?> scheduleAtFixedRate(String context, Runnable runnable, long initialDelay, long period, TimeUnit unit) {
        return executorService.scheduleAtFixedRate(new Task(context, runnable), initialDelay, period, unit);
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
        for (TaskStatistics statistics : taskStatistics.values()) {
            logger.info("stats: {}", statistics);
        }
    }

    private class Task implements Runnable {

        private final String context;
        private final Runnable runnable;

        public Task(String context, Runnable runnable) {
            this.context = context;
            this.runnable = runnable;
        }

        @Override
        public void run() {
            final long start = System.currentTimeMillis();
            try {
                logger.trace("[{}] run", context);
                runnable.run();
            } catch (Throwable e) {
                logger.error("[{}] unexpected error", context, e);
                throw new RuntimeException(e);
            } finally {
                final long totalTime = System.currentTimeMillis() - start;
                if (totalTime > errorThreshold) {
                    logger.error("[{}] complete at {}", context, totalTime);
                } else if (totalTime > warningThreshold) {
                    logger.warn("[{}] complete at {}", context, totalTime);
                } else {
                    logger.trace("[{}] complete at {}", context, totalTime);
                }
                TaskStatistics taskStatistics = ExecutionContext.this.taskStatistics
                    .computeIfAbsent(context, TaskStatistics::new);
                taskStatistics.count++;
                taskStatistics.totalTime += totalTime;
            }
        }
    }

    private class TaskStatistics {
        private final String task;
        private long count;
        private long totalTime;

        private TaskStatistics(String task) {
            this.task = task;
        }

        @Override
        public String toString() {
            return "[" + task + "] count: " + count + " total time: " + totalTime + "ms";
        }
    }
}
