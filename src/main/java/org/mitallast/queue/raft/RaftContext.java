package org.mitallast.queue.raft;

import org.mitallast.queue.common.stream.Streamable;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public interface RaftContext {

    ScheduledFuture schedule(Runnable task, long timeout, TimeUnit timeUnit);

    ScheduledFuture scheduleAtFixedRate(Runnable task, long delay, long timeout, TimeUnit timeUnit);
}
