package org.mitallast.queue.crdt.routing.allocation

import io.vavr.control.Option
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.crdt.routing.RoutingTable

interface AllocationStrategy {

    fun update(routingTable: RoutingTable): Option<Message>
}
