package org.mitallast.queue.raft.log;

import com.google.inject.Inject;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.concurrent.Futures;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.raft.log.entry.EntryFilter;
import org.mitallast.queue.raft.util.ExecutionContext;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class Compactor extends AbstractLifecycleComponent {
    private static final long DEFAULT_COMPACTION_INTERVAL = TimeUnit.HOURS.toMillis(1);
    private static final long COMPACT_INTERVAL = TimeUnit.MINUTES.toMillis(1);

    private final Log log;
    private final EntryFilter filter;
    private final ExecutionContext context;
    private long compactionInterval = DEFAULT_COMPACTION_INTERVAL;
    private long commit;
    private long compact;
    private Compaction compaction;
    private long lastCompaction;
    private CompletableFuture<Void> compactFuture;
    private ScheduledFuture<?> scheduledFuture;

    @Inject
    public Compactor(Settings settings, Log log, EntryFilter filter, ExecutionContext context) {
        super(settings);
        this.log = log;
        this.filter = filter;
        this.context = context;
    }

    public long getCompactionInterval() {
        return compactionInterval;
    }

    public void setCompactionInterval(long compactionInterval) {
        if (compactionInterval < 1)
            throw new IllegalArgumentException("compaction interval must be positive");
        this.compactionInterval = compactionInterval;
    }

    public Compactor withCompactionInterval(long compactionInterval) {
        setCompactionInterval(compactionInterval);
        return this;
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
        scheduledFuture = context.scheduleAtFixedRate(this::compact, COMPACT_INTERVAL, COMPACT_INTERVAL, TimeUnit.MILLISECONDS);
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
