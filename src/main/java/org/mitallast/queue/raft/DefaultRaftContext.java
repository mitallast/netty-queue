package org.mitallast.queue.raft;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class DefaultRaftContext extends AbstractLifecycleComponent implements RaftContext {

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, new DefaultThreadFactory("raft"));
    private final Raft raft;

    @Inject
    public DefaultRaftContext(Config config, Raft raft) {
        super(config, RaftContext.class);
        this.raft = raft;
    }

    @Override
    public ScheduledFuture schedule(Streamable event, long timeout, TimeUnit timeUnit) {
        return scheduler.schedule(() -> raft.apply(event), timeout, timeUnit);
    }

    @Override
    public ScheduledFuture scheduleAtFixedRate(Streamable event, long delay, long timeout, TimeUnit timeUnit) {
        return scheduler.scheduleAtFixedRate(() -> raft.apply(event), delay, timeout, timeUnit);
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
}
