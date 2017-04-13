package org.mitallast.queue.crdt.routing.event;

import org.mitallast.queue.crdt.routing.RoutingTable;

public class RoutingTableChanged {
    private final long index;
    private final RoutingTable routingTable;

    public RoutingTableChanged(long index, RoutingTable routingTable) {
        this.index = index;
        this.routingTable = routingTable;
    }

    public long index() {
        return index;
    }

    public RoutingTable routingTable() {
        return routingTable;
    }
}
