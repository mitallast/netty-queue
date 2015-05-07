package org.mitallast.queue.common.event;

public interface EventListener<Event> {
    public void handle(Event event);
}
