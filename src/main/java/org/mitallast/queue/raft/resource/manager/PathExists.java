package org.mitallast.queue.raft.resource.manager;

import org.mitallast.queue.raft.ConsistencyLevel;
import org.mitallast.queue.raft.Query;
import org.mitallast.queue.raft.resource.result.BooleanResult;

public class PathExists extends PathOperation<BooleanResult, PathExists> implements Query<BooleanResult> {

    public PathExists(String path) {
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

    public static class Builder extends PathOperation.Builder<BooleanResult, Builder, PathExists> {

        @Override
        public PathExists build() {
            return new PathExists(path);
        }
    }
}
