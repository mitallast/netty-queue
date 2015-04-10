package org.mitallast.queue.common.concurrent.futures;

public abstract class AbstractImmediatelyFuture<Type> implements SmartFuture<Type> {

    @Override
    public void invoke(Type result) {
    }

    @Override
    public void invokeException(Throwable ex) {
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return true;
    }
}
