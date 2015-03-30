package org.mitallast.queue.common.concurrent.futures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.function.Consumer;

public class DefaultSmartFuture<Type> implements SmartFuture<Type> {

    private final static Logger logger = LoggerFactory.getLogger(DefaultSmartFuture.class);

    private static final int RUNNING = 0;
    private static final int COMPLETING = 1;
    private static final int COMPLETED = 2;
    private static final int CANCELLED = 4;

    private final Object monitor = new Object();
    private final Sync<Type> sync;
    private Object callbacks;

    public DefaultSmartFuture() {
        sync = new Sync<>();
    }

    @Override
    public void invoke(Type result) {
        boolean resultOfSet = sync.set(result);

        if (resultOfSet) {
            invokeListeners();
        }
    }

    @Override
    public void invokeException(Throwable ex) {
        boolean resultOfSet = sync.setException(ex);

        if (resultOfSet) {
            invokeListeners();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void on(Consumer<FutureResult<Type, Throwable>> listener) {
        if (isDone()) {
            invokeListener(listener);
            return;
        }

        synchronized (monitor) {
            if (callbacks == null) {
                callbacks = listener;
            } else if (callbacks instanceof List) {
                ((List<Consumer<FutureResult<Type, Throwable>>>) callbacks).add(listener);
            } else {
                List<Consumer<FutureResult<Type, Throwable>>> list = new ArrayList<>(2);
                list.add((Consumer<FutureResult<Type, Throwable>>) callbacks);
                list.add(listener);
                callbacks = list;
            }
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        boolean isCanceled = sync.cancel();
        if (isCanceled) {
            invokeListeners();
        }
        return isCanceled;
    }

    @Override
    public Type getOrNull() {
        return sync.value;
    }

    @Override
    public boolean isCancelled() {
        return sync.isCancelled();
    }

    @Override
    public boolean isDone() {
        return sync.isDone();
    }

    @Override
    public boolean isError() {
        return sync.isDone() && sync.exception != null;
    }

    @Override
    public Throwable getError() {
        return sync.exception;
    }

    @Override
    public Type get() throws InterruptedException, ExecutionException {
        try {
            return sync.get();
        } catch (Exception e) {
            invokeListeners();
            throw e;
        }
    }

    @Override
    public Type get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        try {
            return sync.get(unit.toNanos(timeout));
        } catch (Exception e) {
            invokeListeners();
            throw e;
        }
    }

    /**
     * Вызывает листенеров
     */
    @SuppressWarnings("unchecked")
    private void invokeListeners() {
        synchronized (monitor) {
            if (callbacks != null) {
                if (callbacks instanceof List) {
                    List<Consumer<FutureResult<Type, Throwable>>> consumers = (List) callbacks;
                    consumers.forEach(this::invokeListener);
                } else {
                    Consumer<FutureResult<Type, Throwable>> consumer = (Consumer) callbacks;
                    invokeListener(consumer);
                }
            }
        }
    }

    protected void invokeListener(Consumer<FutureResult<Type, Throwable>> consumer) {
        try {
            if (consumer != null) {
                consumer.accept(this);
            }
        } catch (Throwable e) {
            logger.warn("An exception was thrown by {}.accept()", consumer.getClass(), e);
        }
    }

    private static final class Sync<V> extends AbstractQueuedSynchronizer {

        private V value;
        private Throwable exception;

        @Override
        protected int tryAcquireShared(int ignored) {
            if (isDone()) {
                return 1;
            }
            return -1;
        }

        @Override
        protected boolean tryReleaseShared(int finalState) {
            setState(finalState);
            return true;
        }

        V get(long nanos) throws TimeoutException, CancellationException, ExecutionException, InterruptedException {
            if (!tryAcquireSharedNanos(-1, nanos)) {
                throw new TimeoutException("Timeout waiting for task.");
            }

            return getValue();
        }

        V get() throws CancellationException, ExecutionException, InterruptedException {

            // Acquire the shared lock allowing interruption.
            acquireSharedInterruptibly(-1);
            return getValue();
        }

        private V getValue() throws CancellationException, ExecutionException {
            int state = getState();
            switch (state) {
                case COMPLETED:
                    if (exception != null) {
                        throw new ExecutionException(exception);
                    } else {
                        return value;
                    }

                case CANCELLED:
                    throw new CancellationException("Task was cancelled.");

                default:
                    throw new IllegalStateException("Error, synchronizer in invalid state: " + state);
            }
        }

        boolean isDone() {
            return (getState() & (COMPLETED | CANCELLED)) != 0;
        }

        boolean isCancelled() {
            return getState() == CANCELLED;
        }

        boolean set(V v) {
            return complete(v, null, COMPLETED);
        }

        boolean setException(Throwable t) {
            return complete(null, t, COMPLETED);
        }

        boolean cancel() {
            return complete(null, null, CANCELLED);
        }

        private boolean complete(V v, Throwable t, int finalState) {
            boolean doCompletion = compareAndSetState(RUNNING, COMPLETING);
            if (doCompletion) {
                this.value = v;
                this.exception = t;
                releaseShared(finalState);
            } else if (getState() == COMPLETING) {
                acquireShared(-1);
            }
            return doCompletion;
        }
    }
}
