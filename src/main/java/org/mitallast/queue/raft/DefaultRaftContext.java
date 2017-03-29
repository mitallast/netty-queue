package org.mitallast.queue.raft;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;

import java.io.IOException;
import java.util.concurrent.*;

public class DefaultRaftContext extends AbstractLifecycleComponent implements RaftContext {
    private final static Logger logger = LogManager.getLogger();

    private final ConcurrentMap<String, ScheduledFuture> timers;
    private final ScheduledExecutorService scheduler;

    @Inject
    public DefaultRaftContext(Config config) {
        timers = new ConcurrentHashMap<>();
        scheduler = Executors.newScheduledThreadPool(1, new DefaultThreadFactory("raft"));
    }

    @Override
    protected void doStart() throws IOException {
    }

    @Override
    protected void doStop() throws IOException {
    }

    @Override
    protected void doClose() throws IOException {
        scheduler.shutdown();
    }

    @Override
    public void setTimer(String name, long delayMs, Runnable task) {
        timers.compute(name, (i, prev) -> {
            if (prev != null) {
                prev.cancel(true);
            }
            return scheduler.schedule(task, delayMs, TimeUnit.MILLISECONDS);
        });
    }

    @Override
    public void startTimer(String name, long delayMs, long periodMs, Runnable task) {
        timers.compute(name, (i, prev) -> {
            if (prev != null) {
                prev.cancel(true);
            }
            return scheduler.scheduleAtFixedRate(task, delayMs, periodMs, TimeUnit.MILLISECONDS);
        });
    }

    @Override
    public void cancelTimer(String name) {
        ScheduledFuture prev = timers.remove(name);
        if (prev != null) {
            prev.cancel(true);
        }
    }
}
