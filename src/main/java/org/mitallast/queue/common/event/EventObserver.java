package org.mitallast.queue.common.event;

public interface EventObserver<Event> {

    static <Event> EventObserver<Event> create() {
        return new ListEventObserver<>();
    }

    void addListener(EventListener<Event> listener);

    void removeListener(EventListener<Event> listener);

    void triggerEvent(Event event);
}
