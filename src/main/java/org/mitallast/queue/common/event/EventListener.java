package org.mitallast.queue.common.event;

public interface EventListener<Event> {
    void handle(Event event);
}
