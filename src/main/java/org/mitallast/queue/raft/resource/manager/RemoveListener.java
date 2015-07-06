package org.mitallast.queue.raft.resource.manager;

import org.mitallast.queue.raft.Command;
import org.mitallast.queue.raft.resource.result.BooleanResult;

public class RemoveListener extends PathOperation<BooleanResult, RemoveListener> implements Command<BooleanResult> {

    public RemoveListener(String path) {
        super(path);
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends PathOperation.Builder<BooleanResult, Builder, RemoveListener> {

        @Override
        public RemoveListener build() {
            return new RemoveListener(path);
        }
    }
}
