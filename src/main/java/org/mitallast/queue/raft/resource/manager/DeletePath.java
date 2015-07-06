package org.mitallast.queue.raft.resource.manager;

import org.mitallast.queue.raft.Command;
import org.mitallast.queue.raft.resource.result.BooleanResult;

public class DeletePath extends PathOperation<BooleanResult, DeletePath> implements Command<BooleanResult> {

    public DeletePath(String path) {
        super(path);
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends PathOperation.Builder<BooleanResult, Builder, DeletePath> {

        @Override
        public DeletePath build() {
            return new DeletePath(path);
        }
    }
}
