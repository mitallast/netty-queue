package org.mitallast.queue.common.concurrent.futures;

import com.google.common.base.Preconditions;
import org.mitallast.queue.common.concurrent.Listener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

public class DefaultSmartFuture<Type> extends AbstractQueuedSynchronizer implements SmartFuture<Type> {

    private final static Logger logger = LoggerFactory.getLogger(DefaultSmartFuture.class);

    private final static AtomicReferenceFieldUpdater<DefaultSmartFuture, Node> headUpdater =
        AtomicReferenceFieldUpdater.newUpdater(DefaultSmartFuture.class, Node.class, "head");
    private final static AtomicReferenceFieldUpdater<DefaultSmartFuture, Node> tailUpdater =
        AtomicReferenceFieldUpdater.newUpdater(DefaultSmartFuture.class, Node.class, "tail");

    private static final int RUNNING = 0;
    private static final int COMPLETING = 1;
    private static final int COMPLETED = 2;
    private static final int CANCELLED = 4;

    private volatile Type value;
    private volatile Throwable exception;

    private volatile Node<Type> head;
    private volatile Node<Type> tail;

    public DefaultSmartFuture() {
        head = tail = new Node<>(null);
    }

    private boolean offer(Listener<Type> listener) {
        final Node<Type> newNode = new Node<>(listener);
        for (Node<Type> t = tail, p = t; ; ) {
            Node<Type> q = p.next;
            if (q == null) {
                // p is last node
                if (p.casNext(null, newNode)) {
                    // Successful CAS is the linearization point
                    // for e to become an element of this queue,
                    // and for newNode to become "live".
                    if (p != t) // hop two nodes at a time
                        casTail(t, newNode);  // Failure is OK.
                    return true;
                }
                // Lost CAS race to another thread; re-read next
            } else if (p == q)
                // We have fallen off list.  If tail is unchanged, it
                // will also be off-list, in which case we need to
                // jump to head, from which all live nodes are always
                // reachable.  Else the new tail is a better bet.
                p = (t != (t = tail)) ? t : head;
            else
                // Check for tail updates after two hops.
                p = (p != t && t != (t = tail)) ? t : q;
        }
    }

    private Listener<Type> poll() {
        restartFromHead:
        for (; ; ) {
            for (Node<Type> h = head, p = h, q; ; ) {
                Listener<Type> item = p.item;

                if (item != null && p.casItem(item, null)) {
                    // Successful CAS is the linearization point
                    // for item to be removed from this queue.
                    if (p != h) // hop two nodes at a time
                        updateHead(h, ((q = p.next) != null) ? q : p);
                    return item;
                } else if ((q = p.next) == null) {
                    updateHead(h, p);
                    return null;
                } else if (p == q)
                    continue restartFromHead;
                else
                    p = q;
            }
        }
    }

    private void updateHead(Node<Type> h, Node<Type> p) {
        if (h != p && casHead(h, p))
            h.lazySetNext(h);
    }

    private boolean casTail(Node<Type> cmp, Node<Type> val) {
        return tailUpdater.compareAndSet(this, cmp, val);
    }

    private boolean casHead(Node<Type> cmp, Node<Type> val) {
        return headUpdater.compareAndSet(this, cmp, val);
    }

    @Override
    public void invoke(Type result) {
        Preconditions.checkNotNull(result);
        if (complete(result, null, COMPLETED)) {
            invokeListeners();
        }
    }

    @Override
    public void invokeException(Throwable ex) {
        if (complete(null, ex, COMPLETED)) {
            invokeListeners();
        }
    }

    @Override
    public void on(Listener<Type> listener) {
        if (isDone()) {
            invokeListener(listener);
            return;
        }

        offer(listener);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        boolean isCanceled = complete(null, null, CANCELLED);
        if (isCanceled) {
            invokeListeners();
        }
        return isCanceled;
    }

    private boolean isError() {
        return isDone() && exception != null;
    }

    @Override
    public Type get() throws InterruptedException, ExecutionException {
        try {
            // Acquire the shared lock allowing interruption.
            acquireSharedInterruptibly(-1);
            return getValue();
        } catch (Exception e) {
            invokeListeners();
            throw e;
        }
    }

    @Override
    public Type get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        try {
            if (!tryAcquireSharedNanos(-1, unit.toNanos(timeout))) {
                throw new TimeoutException("Timeout waiting for task.");
            }
            return getValue();
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
        Listener<Type> poll;
        while ((poll = poll()) != null) {
            invokeListener(poll);
        }
    }

    protected void invokeListener(Listener<Type> listener) {
        try {
            if (isError()) {
                listener.onFailure(exception);
            } else {
                listener.onResponse(value);
            }
        } catch (Throwable e) {
            logger.warn("An exception was thrown by {}.accept()", listener.getClass(), e);
        }
    }

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

    @Override
    public boolean isDone() {
        return (getState() & (COMPLETED | CANCELLED)) != 0;
    }

    @Override
    public boolean isCancelled() {
        return getState() == CANCELLED;
    }

    private Type getValue() throws CancellationException, ExecutionException {
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

    private boolean complete(Type v, Throwable t, int finalState) {
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

    private final static class Node<Type> {
        private final static AtomicReferenceFieldUpdater<Node, Node> nextUpdater =
            AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "next");
        private final static AtomicReferenceFieldUpdater<Node, Listener> itemUpdater =
            AtomicReferenceFieldUpdater.newUpdater(Node.class, Listener.class, "item");

        private volatile Listener<Type> item;
        private volatile Node<Type> next;

        private Node(Listener<Type> item) {
            this.item = item;
            this.next = null;
        }

        private boolean casItem(Listener<Type> cmp, Listener<Type> val) {
            return itemUpdater.compareAndSet(this, cmp, val);
        }

        private void lazySetNext(Node<Type> val) {
            nextUpdater.lazySet(this, val);
        }

        private boolean casNext(Node<Type> cmp, Node<Type> val) {
            return nextUpdater.compareAndSet(this, cmp, val);
        }
    }
}
