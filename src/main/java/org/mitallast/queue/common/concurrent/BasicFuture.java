package org.mitallast.queue.common.concurrent;

import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class BasicFuture<T> implements Future<T> {

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private volatile Object reply;
    private volatile State state = State.WAITING;

    public void set(T reply) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            this.reply = reply;
            state = State.DONE;
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    public void setException(Exception exception) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            this.reply = exception;
            state = State.ERROR;
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        state = State.CANCELLED;
        return true;
    }

    @Override
    public boolean isCancelled() {
        return state == State.CANCELLED;
    }

    @Override
    public boolean isDone() {
        return state == State.DONE;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            while (reply == null)
                notEmpty.await();
            return report();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        long nanos = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            while (reply == null) {
                if (nanos <= 0)
                    throw new TimeoutException();
                nanos = notEmpty.awaitNanos(nanos);
            }
            return report();
        } finally {
            lock.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    private T report() throws ExecutionException {
        State state = this.state;
        Object reply = this.reply;
        if (state == State.DONE)
            return (T) reply;
        if (state == State.CANCELLED)
            throw new CancellationException();
        throw new ExecutionException((Throwable) reply);
    }

    private static enum State {WAITING, DONE, CANCELLED, ERROR}
}
