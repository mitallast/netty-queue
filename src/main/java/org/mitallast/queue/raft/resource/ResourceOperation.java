package org.mitallast.queue.raft.resource;


import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.Operation;

public abstract class ResourceOperation<T extends Operation<U>, U extends Streamable> implements Operation<U> {
    protected final long resource;
    protected final T operation;

    public ResourceOperation(long resource, T operation) {
        this.resource = resource;
        this.operation = operation;
    }

    public long resource() {
        return resource;
    }

    public T operation() {
        return operation;
    }
}
