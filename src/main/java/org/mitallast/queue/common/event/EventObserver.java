package org.mitallast.queue.common.event;

public interface EventObserver<Event> {

    public static <Event> EventObserver<Event> create() {
        return new ListEventObserver<>();
    }

    public void addListener(EventListener<Event> listener);

    public void removeListener(EventListener<Event> listener);

    public void triggerEvent(Event event);
}
