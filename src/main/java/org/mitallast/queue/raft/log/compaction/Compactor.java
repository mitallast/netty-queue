package org.mitallast.queue.raft.log.compaction;

import com.google.inject.Inject;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.concurrent.Futures;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.unit.TimeValue;
import org.mitallast.queue.raft.util.ExecutionContext;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class Compactor extends AbstractLifecycleComponent {
    private final ExecutionContext context;
    private final long compactionInterval;
    private final MinorCompactionFactory minorCompactionFactory;
    private final MajorCompactionFactory majorCompactionFactory;
    private volatile long commit;
    private volatile long compact;
    private volatile Compaction compaction;
    private volatile long lastCompaction;
    private volatile CompletableFuture<Void> compactFuture;
    private volatile ScheduledFuture<?> scheduledFuture;

    @Inject
    public Compactor(Settings settings, ExecutionContext context, MinorCompactionFactory minorCompactionFactory, MajorCompactionFactory majorCompactionFactory) {
        super(settings);
        this.context = context;
        this.minorCompactionFactory = minorCompactionFactory;
        this.majorCompactionFactory = majorCompactionFactory;
        this.compactionInterval = componentSettings.getAsTime("interval", TimeValue.timeValueHours(1)).millis();
    }

    public void setCommitIndex(long index) {
        this.commit = index;
    }

    public void setCompactIndex(long index) {
        this.compact = index;
    }

    public synchronized CompletableFuture<Void> compact() {
        if (compactFuture != null) {
            return compactFuture;
        }

        compactFuture = CompletableFuture.supplyAsync(() -> {
            if (compaction == null) {
                if (System.currentTimeMillis() - lastCompaction > compactionInterval) {
                    compaction = majorCompactionFactory.create(compact);
                    lastCompaction = System.currentTimeMillis();
                } else {
                    compaction = minorCompactionFactory.create(commit);
                }
                return compaction;
            }
            return null;
        }, context.executor("compact")).thenCompose(c -> {
            if (compaction != null) {
                return compaction.run().thenRun(() -> {
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
        scheduledFuture = context.scheduleAtFixedRate("compaction timer", this::compact, compactionInterval, compactionInterval, TimeUnit.MILLISECONDS);
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
