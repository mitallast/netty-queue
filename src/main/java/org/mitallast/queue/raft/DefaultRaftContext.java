package org.mitallast.queue.raft;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.internal.PlatformDependent;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultRaftContext extends AbstractLifecycleComponent implements RaftContext, Runnable {

    private static final int NONE = 0;
    private static final int SUBMITTED = 1;
    private static final int RUNNING = 2;
    private final Queue<Streamable> queue;
    private final DefaultEventExecutor executor;
    private final DefaultEventExecutor schedule;
    private final Raft raft;
    private final AtomicInteger state = new AtomicInteger();
    private final int maxTaskExecutePerRun;

    @Inject
    public DefaultRaftContext(Config config, Raft raft) {
        super(config, RaftContext.class);
        this.raft = raft;
        queue = PlatformDependent.newMpscQueue();

        executor = new DefaultEventExecutor(new DefaultThreadFactory("raft", true));
        schedule = new DefaultEventExecutor(new DefaultThreadFactory("raft.schedule", true));
        maxTaskExecutePerRun = 1024;
    }

    @Override
    public void run() {
        if (!state.compareAndSet(SUBMITTED, RUNNING)) {
            return;
        }
        for (; ; ) {
            int i = 0;
            try {
                for (; i < maxTaskExecutePerRun; i++) {
                    Streamable task = queue.poll();
                    if (task == null) {
                        break;
                    }
                    raft.handle(task);
                }
            } finally {
                if (i == maxTaskExecutePerRun) {
                    try {
                        state.set(SUBMITTED);
                        executor.execute(this);
                        //noinspection ReturnInsideFinallyBlock
                        return; // done
                    } catch (Throwable ignore) {
                        // Reset the state back to running as we will keep on executing tasks.
                        state.set(RUNNING);
                        // if an error happened we should just ignore it and let the loop run again as there is not
                        // much else we can do. Most likely this was triggered by a full task queue. In this case
                        // we just will run more tasks and try again later.
                    }
                } else {
                    state.set(NONE);
                    //noinspection ReturnInsideFinallyBlock
                    return; // done
                }
            }
        }
    }

    @Override
    public void submit(Streamable event) {
        if (!queue.offer(event)) {
            throw new RejectedExecutionException();
        }
        if (state.compareAndSet(NONE, SUBMITTED)) {
            // Actually it could happen that the runnable was picked up in between but we not care to much and just
            // execute ourself. At worst this will be a NOOP when run() is called.
            try {
                executor.execute(this);
            } catch (Throwable e) {
                // Not reset the state as some other Runnable may be added to the queue already in the meantime.
                queue.remove(event);
                PlatformDependent.throwException(e);
            }
        }
    }

    @Override
    public ScheduledFuture schedule(Streamable event, long timeout, TimeUnit timeUnit) {
        return schedule.schedule(() -> raft.handle(event), timeout, timeUnit);
    }

    @Override
    public ScheduledFuture scheduleAtFixedRate(Streamable event, long delay, long timeout, TimeUnit timeUnit) {
        return schedule.scheduleAtFixedRate(() -> raft.handle(event), delay, timeout, timeUnit);
    }

    @Override
    protected void doStart() throws IOException {

    }

    @Override
    protected void doStop() throws IOException {

    }

    @Override
    protected void doClose() throws IOException {
        executor.shutdownGracefully();
    }
}
