package org.mitallast.queue.common;

import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.mitallast.queue.common.concurrent.NamedExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

public class BaseTest {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected final Random random = new Random();

    private final static int availableProcessors = Runtime.getRuntime().availableProcessors();

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private ExecutorService executorService = Executors.newCachedThreadPool(NamedExecutors.newThreadFactory("test"));

    @After
    public void tearDownExecutorService() throws Exception {
        executorService.shutdownNow();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
        assert executorService.isTerminated();
    }

    protected int concurrency() {
        return availableProcessors;
    }

    protected int max() {
        return 20000;
    }

    protected final int total() {
        return max() * concurrency();
    }

    protected void executeConcurrent(Task task) throws Exception {
        final List<Future> futures = new ArrayList<>(concurrency());
        for (int i = 0; i < concurrency(); i++) {
            Future future = executorService.submit(() -> {
                try {
                    task.execute();
                } catch (Exception e) {
                    assert false : e;
                }
            });
            futures.add(future);
        }
        for (Future future : futures) {
            future.get();
        }
    }

    protected void executeConcurrent(ThreadTask task) throws Exception {
        final List<Future> futures = new ArrayList<>(concurrency());
        for (int i = 0; i < concurrency(); i++) {
            final int thread = i;
            Future future = executorService.submit(() -> {
                try {
                    task.execute(thread, concurrency());
                } catch (Exception e) {
                    assert false : e;
                }
            });
            futures.add(future);
        }
        for (Future future : futures) {
            future.get();
        }
    }

    protected <T> Future<T> submit(Callable<T> callable) {
        return executorService.submit(() -> {
            try {
                return callable.call();
            } catch (Exception e) {
                assert false : e;
                throw e;
            }
        });
    }

    protected Future<Void> submit(Runnable runnable) {
        return executorService.submit(runnable, null);
    }

    protected void printQps(String metric, long total, long start, long end) {
        long qps = (long) (total / (double) (end - start) * 1000.);
        logger.info(metric + ": " + total + " at " + (end - start) + "ms");
        logger.info(metric + ": " + qps + " qps");
    }

    public interface Task {
        void execute() throws Exception;
    }

    public interface ThreadTask {
        void execute(int thread, int concurrency) throws Exception;
    }
}
