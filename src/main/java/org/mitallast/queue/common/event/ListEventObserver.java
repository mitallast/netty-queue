package org.mitallast.queue.common.event;

import java.util.concurrent.CopyOnWriteArrayList;

public class ListEventObserver<Event> extends CopyOnWriteArrayList<EventListener<Event>> implements EventObserver<Event> {

    @Override
    public void addListener(EventListener<Event> listener) {
        add(listener);
    }

    @Override
    public void removeListener(EventListener<Event> listener) {
        remove(listener);
    }

    @Override
    public void triggerEvent(Event event) {
        forEach(listener -> listener.handle(event));
    }
}
