package org.mitallast.queue.crdt.routing.allocation;

import javaslang.control.Option;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.crdt.routing.RoutingTable;

public interface AllocationStrategy {

    Option<Streamable> update(RoutingTable routingTable);
}
