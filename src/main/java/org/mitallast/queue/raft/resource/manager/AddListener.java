package org.mitallast.queue.raft.resource.manager;

import org.mitallast.queue.raft.Command;
import org.mitallast.queue.raft.resource.result.BooleanResult;

public class AddListener extends PathOperation<BooleanResult, AddListener> implements Command<BooleanResult> {

    public AddListener(String path) {
        super(path);
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends PathOperation.Builder<BooleanResult, Builder, AddListener> {

        @Override
        public AddListener build() {
            return new AddListener(path);
        }
    }
}
