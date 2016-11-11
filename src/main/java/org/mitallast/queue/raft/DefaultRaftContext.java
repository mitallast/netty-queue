package org.mitallast.queue.raft;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;

import java.io.IOException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class DefaultRaftContext extends AbstractLifecycleComponent implements RaftContext {

    private final DefaultEventExecutor executor;

    @Inject
    public DefaultRaftContext(Config config) {
        super(config, RaftContext.class);
        executor = new DefaultEventExecutor(new DefaultThreadFactory("raft", true, Thread.MAX_PRIORITY));
    }

    @Override
    public void execute(Runnable runnable) {
        executor.execute(runnable);
    }

    @Override
    public ScheduledFuture schedule(Runnable runnable, long timeout, TimeUnit timeUnit) {
        return executor.schedule(runnable, timeout, timeUnit);
    }

    @Override
    public ScheduledFuture scheduleAtFixedRate(Runnable runnable, long delay, long timeout, TimeUnit timeUnit) {
        return executor.scheduleAtFixedRate(runnable, delay, timeout, timeUnit);
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
