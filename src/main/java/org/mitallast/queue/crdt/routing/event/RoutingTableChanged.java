package org.mitallast.queue.crdt.routing.event;

import org.mitallast.queue.crdt.routing.RoutingTable;

public class RoutingTableChanged {
    private final RoutingTable prev;
    private final RoutingTable next;

    public RoutingTableChanged(RoutingTable prev, RoutingTable next) {
        this.prev = prev;
        this.next = next;
    }

    public RoutingTable prev() {
        return prev;
    }

    public RoutingTable next() {
        return next;
    }
}
