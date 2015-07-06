package org.mitallast.queue.raft.resource.manager;

import org.mitallast.queue.raft.Command;
import org.mitallast.queue.raft.resource.result.BooleanResult;

public class CreatePath extends PathOperation<BooleanResult, CreatePath> implements Command<BooleanResult> {

    public CreatePath(String path) {
        super(path);
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends PathOperation.Builder<BooleanResult, Builder, CreatePath> {

        @Override
        public CreatePath build() {
            return new CreatePath(path);
        }
    }
}
