package org.mitallast.queue.raft;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public interface RaftContext {

    void execute(Runnable runnable);

    ScheduledFuture schedule(Runnable runnable, long timeout, TimeUnit timeUnit);

    ScheduledFuture scheduleAtFixedRate(Runnable runnable, long delay, long timeout, TimeUnit timeUnit);
}
