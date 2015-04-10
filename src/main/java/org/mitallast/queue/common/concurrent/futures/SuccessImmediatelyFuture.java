package org.mitallast.queue.common.concurrent.futures;

import org.mitallast.queue.common.concurrent.Listener;

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
    public boolean isDone() {
        return true;
    }

    @Override
    public void on(Listener<T> listener) {
        listener.onResponse(value);
    }
}
