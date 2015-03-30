package org.mitallast.queue.common.concurrent.futures;

import java.util.concurrent.TimeUnit;

public class SuccessImmediatelyFuture<T> extends AbstractImmediatelyFuture<T> {
    private final T value;

    public SuccessImmediatelyFuture(T value) {
        this.value = value;
    }

    @Override
    public T get() {
        return value;
    }

    @Override
    public T get(long timeout, TimeUnit unit) {
        return value;
    }

    @Override
    public T getOrNull() {
        return value;
    }

    @Override
    public boolean isDone() {
        return true;
    }


    @Override
    public boolean isError() {
        return false;
    }

    @Override
    public Throwable getError() {
        return null;
    }
}
