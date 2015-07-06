package org.mitallast.queue.raft.resource.manager;

import org.mitallast.queue.raft.ConsistencyLevel;
import org.mitallast.queue.raft.Query;
import org.mitallast.queue.raft.resource.result.StringListResult;

public class PathChildren extends PathOperation<StringListResult, PathChildren> implements Query<StringListResult> {

    public PathChildren(String path) {
        super(path);
    }

    @Override
    public ConsistencyLevel consistency() {
        return ConsistencyLevel.LINEARIZABLE_STRICT;
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends PathOperation.Builder<StringListResult, Builder, PathChildren> {

        @Override
        public PathChildren build() {
            return new PathChildren(path);
        }
    }
}
