package org.mitallast.queue.raft.log;

import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;

import java.util.concurrent.CompletableFuture;

public abstract class Compaction extends AbstractComponent {
    private final long index;
    private boolean running = true;

    protected Compaction(Settings settings, long index) {
        super(settings);
        this.index = index;
    }

    public abstract Type type();

    public long index() {
        return index;
    }

    public boolean isRunning() {
        return running;
    }

    protected void setRunning(boolean running) {
        this.running = running;
    }

    public boolean isComplete() {
        return !running;
    }

    abstract CompletableFuture<Void> run(SegmentManager segments);

    public enum Type {

        MINOR(false),

        MAJOR(true);

        private boolean ordered;

        Type(boolean ordered) {
            this.ordered = ordered;
        }

        public boolean isOrdered() {
            return ordered;
        }
    }

}
