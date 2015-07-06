package org.mitallast.queue.raft.resource;

import java.util.function.Consumer;

@FunctionalInterface
public interface EventListener extends Consumer<Event> {
}
