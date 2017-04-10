package org.mitallast.queue.common.events;

import com.google.common.collect.ImmutableMultimap;
import org.mitallast.queue.common.Immutable;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class DefaultEventBus implements EventBus {
    private final ReentrantLock lock = new ReentrantLock();
    private volatile ImmutableMultimap<Class, Consumer> consumers = ImmutableMultimap.of();

    @Override
    public <Event> void subscribe(Class<Event> eventClass, Consumer<Event> consumer) {
        lock.lock();
        try {
            consumers = Immutable.compose(consumers, eventClass, consumer);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public <Event> void unsubscribe(Class<Event> eventClass, Consumer<Event> consumer) {
        lock.lock();
        try {
            consumers = Immutable.subtract(consumers, eventClass, consumer);
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
}
