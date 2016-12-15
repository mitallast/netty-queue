package org.mitallast.queue.raft;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;
import java.util.concurrent.*;

public class DefaultRaftContext extends AbstractLifecycleComponent implements RaftContext {

    private final ArrayBlockingQueue<Streamable> queue = new ArrayBlockingQueue<>(65536);
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final Thread thread;

    @Inject
    public DefaultRaftContext(Config config, Raft raft) {
        super(config, RaftContext.class);
        thread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Streamable event = queue.take();
                    if (event != null) {
                        raft.handle(event);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        thread.setName("raft");
        thread.setDaemon(true);
        thread.start();
    }

    @Override
    public void submit(Streamable event) {
        try {
            queue.put(event);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("event not offered", e);
        }
    }

    @Override
    public ScheduledFuture schedule(Streamable event, long timeout, TimeUnit timeUnit) {
        return scheduler.schedule(() -> submit(event), timeout, timeUnit);
    }

    @Override
    public ScheduledFuture scheduleAtFixedRate(Streamable event, long delay, long timeout, TimeUnit timeUnit) {
        return scheduler.scheduleAtFixedRate(() -> submit(event), delay, timeout, timeUnit);
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
        thread.interrupt();
    }
}
