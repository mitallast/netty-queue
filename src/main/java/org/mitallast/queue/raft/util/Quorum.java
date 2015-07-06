package org.mitallast.queue.raft.util;

import java.util.function.Consumer;

public class Quorum {
    private final int quorum;
    private int succeeded;
    private int failed;
    private Consumer<Boolean> callback;
    private boolean complete;

    public Quorum(int quorum, Consumer<Boolean> callback) {
        this.quorum = quorum;
        this.callback = callback;
    }

    private void checkComplete() {
        if (!complete && callback != null) {
            if (succeeded >= quorum) {
                complete = true;
                callback.accept(true);
            } else if (failed >= quorum) {
                complete = true;
                callback.accept(false);
            }
        }
    }

    public Quorum succeed() {
        succeeded++;
        checkComplete();
        return this;
    }

    public Quorum fail() {
        failed++;
        checkComplete();
        return this;
    }

    public void cancel() {
        callback = null;
        complete = true;
    }

}
