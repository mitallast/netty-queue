package org.mitallast.queue.raft.log;

import com.google.inject.Inject;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.concurrent.Futures;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.unit.TimeValue;
import org.mitallast.queue.raft.log.entry.EntryFilter;
import org.mitallast.queue.raft.util.ExecutionContext;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class Compactor extends AbstractLifecycleComponent {
    private final Log log;
    private final EntryFilter filter;
    private final ExecutionContext context;
    private final long compactionInterval;
    private volatile long commit;
    private volatile long compact;
    private volatile Compaction compaction;
    private volatile long lastCompaction;
    private volatile CompletableFuture<Void> compactFuture;
    private volatile ScheduledFuture<?> scheduledFuture;

    @Inject
    public Compactor(Settings settings, Log log, EntryFilter filter, ExecutionContext context) {
        super(settings);
        this.log = log;
        this.filter = filter;
        this.context = context;
        this.compactionInterval = componentSettings.getAsTime("interval", TimeValue.timeValueHours(1)).millis();
    }

    public void setCommitIndex(long index) {
        this.commit = index;
    }

    public void setCompactIndex(long index) {
        this.compact = index;
    }

    synchronized CompletableFuture<Void> compact() {
        if (compactFuture != null) {
            return compactFuture;
        }

        compactFuture = CompletableFuture.supplyAsync(() -> {
            if (compaction == null) {
                if (System.currentTimeMillis() - lastCompaction > compactionInterval) {
                    compaction = new MajorCompaction(settings, compact, filter, context);
                    lastCompaction = System.currentTimeMillis();
                } else {
                    compaction = new MinorCompaction(settings, commit, filter, context);
                }
                return compaction;
            }
            return null;
        }, context.executor()).thenCompose(c -> {
            if (compaction != null) {
                return compaction.run(log.segments).thenRun(() -> {
                    synchronized (this) {
                        compactFuture = null;
                    }
                    compaction = null;
                });
            }
            return Futures.complete(null);
        });
        return compactFuture;
    }

    @Override
    protected void doStart() throws IOException {
        scheduledFuture = context.scheduleAtFixedRate(this::compact, compactionInterval, compactionInterval, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void doStop() throws IOException {
        if (compactFuture != null) {
            compactFuture.cancel(false);
            compactFuture = null;
        }
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
            scheduledFuture = null;
        }
    }

    @Override
    protected void doClose() throws IOException {
    }
}
