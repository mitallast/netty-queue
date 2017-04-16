package org.mitallast.queue.raft;

import com.google.inject.Inject;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;

import java.util.concurrent.*;

public class DefaultRaftContext extends AbstractLifecycleComponent implements RaftContext {

    private final ConcurrentMap<String, ScheduledFuture> timers;
    private final ScheduledExecutorService scheduler;

    @Inject
    public DefaultRaftContext() {
        timers = new ConcurrentHashMap<>();
        scheduler = Executors.newScheduledThreadPool(1, new DefaultThreadFactory("raft"));
    }

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
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
