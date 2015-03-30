package org.mitallast.queue.common.concurrent.futures;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class FailImmediatelyFuture<T> extends AbstractImmediatelyFuture<T> {

    private final Throwable value;

    public FailImmediatelyFuture(Throwable value) {
        this.value = value;
    }

    @Override
    public T get() throws ExecutionException {
        if (value instanceof ExecutionException) {
            throw (ExecutionException) value;
        }
        throw new ExecutionException(value);
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws ExecutionException {
        return get();
    }

    @Override
    public T getOrNull() {
        return null;
    }

    @Override
    public boolean isDone() {
        return false;
    }


    @Override
    public boolean isError() {
        return true;
    }

    @Override
    public Throwable getError() {
        return value;
    }
}