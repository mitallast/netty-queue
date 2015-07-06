package org.mitallast.queue.raft.resource.manager;

import org.mitallast.queue.raft.ConsistencyLevel;
import org.mitallast.queue.raft.Query;
import org.mitallast.queue.raft.resource.result.BooleanResult;

public class ResourceExists extends PathOperation<BooleanResult, ResourceExists> implements Query<BooleanResult> {

    public ResourceExists(String path) {
        super(path);
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    @Override
    public ConsistencyLevel consistency() {
        return ConsistencyLevel.LINEARIZABLE_STRICT;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends PathOperation.Builder<BooleanResult, Builder, ResourceExists> {

        @Override
        public ResourceExists build() {
            return new ResourceExists(path);
        }
    }
}
