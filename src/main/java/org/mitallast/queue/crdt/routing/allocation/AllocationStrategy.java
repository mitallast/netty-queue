package org.mitallast.queue.crdt.routing.allocation;

import javaslang.control.Option;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.crdt.routing.RoutingTable;

public interface AllocationStrategy {

    Option<Message> update(RoutingTable routingTable);
}
