package org.mitallast.queue.common.events;

import com.google.common.collect.ImmutableSetMultimap;
import org.mitallast.queue.common.Immutable;

import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class DefaultEventBus implements EventBus {
    private final ReentrantLock lock = new ReentrantLock();
    private volatile ImmutableSetMultimap<Class, Listener> consumers = ImmutableSetMultimap.of();

    public <Event> void subscribe(Class<Event> eventClass, Consumer<Event> consumer) {
        lock.lock();
        try {
            consumers = Immutable.compose(consumers, eventClass, new Listener<>(consumer));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public <Event> void subscribe(Class<Event> eventClass, Consumer<Event> consumer, Executor executor) {
        lock.lock();
        try {
            consumers = Immutable.compose(consumers, eventClass, new AsyncListener<>(consumer, executor));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public <Event> void unsubscribe(Class<Event> eventClass, Consumer<Event> consumer) {
        lock.lock();
        try {
            consumers = Immutable.subtract(consumers, eventClass, new Listener<>(consumer));
        } finally {
            lock.unlock();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <Event> void trigger(Event event) {
        consumers.get(event.getClass()).forEach(consumer -> consumer.accept(event));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <Event> void trigger(Class<Event> eventClass, Event event) {
        consumers.get(eventClass).forEach(consumer -> consumer.accept(event));
    }

    private static class Listener<Event> {
        protected final Consumer<Event> consumer;

        private Listener(Consumer<Event> consumer) {
            this.consumer = consumer;
        }

        public void accept(Event event) {
            consumer.accept(event);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Listener listener = (Listener) o;

            return consumer != null ? consumer.equals(listener.consumer) : listener.consumer == null;
        }

        @Override
        public int hashCode() {
            return consumer != null ? consumer.hashCode() : 0;
        }
    }

    private static class AsyncListener<Event> extends Listener<Event> {
        private final Executor executor;

        public AsyncListener(Consumer<Event> consumer, Executor executor) {
            super(consumer);
            this.executor = executor;
        }

        @Override
        public void accept(Event event) {
            executor.execute(() -> consumer.accept(event));
        }
    }
}
