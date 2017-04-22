package org.mitallast.queue.common.events;

import javaslang.collection.HashMultimap;
import javaslang.collection.Multimap;

import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class DefaultEventBus implements EventBus {
    private final ReentrantLock lock = new ReentrantLock();
    private volatile Multimap<Class, Listener> consumers = HashMultimap.withSet().empty();

    public <Event> void subscribe(Class<Event> eventClass, Consumer<Event> consumer) {
        lock.lock();
        try {
            consumers = consumers.put(eventClass, new Listener<>(consumer));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public <Event> void subscribe(Class<Event> eventClass, Consumer<Event> consumer, Executor executor) {
        lock.lock();
        try {
            consumers = consumers.put(eventClass, new Listener<>(consumer, executor));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public <Event> void unsubscribe(Class<Event> eventClass, Consumer<Event> consumer) {
        lock.lock();
        try {
            consumers = consumers.remove(eventClass, new Listener<>(consumer));
        } finally {
            lock.unlock();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <Event> void trigger(Event event) {
        consumers.get(event.getClass()).forEach(consumers -> consumers.forEach(consumer -> consumer.accept(event)));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <Event> void trigger(Class<Event> eventClass, Event event) {
        consumers.get(eventClass).forEach(consumers -> consumers.forEach(consumer -> consumer.accept(event)));
    }

    private static class Listener<Event> {
        private final Consumer<Event> consumer;
        private final Executor executor;

        public Listener(Consumer<Event> consumer) {
            this(consumer, null);
        }

        public Listener(Consumer<Event> consumer, Executor executor) {
            this.consumer = consumer;
            this.executor = executor;
        }

        public void accept(Event event) {
            if (executor == null) {
                consumer.accept(event);
            } else {
                executor.execute(() -> consumer.accept(event));
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Listener<?> that = (Listener<?>) o;

            return consumer.equals(that.consumer);
        }

        @Override
        public int hashCode() {
            return consumer.hashCode();
        }
    }
}
