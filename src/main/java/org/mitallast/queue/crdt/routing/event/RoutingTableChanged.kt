package org.mitallast.queue.crdt.routing.event

import org.mitallast.queue.crdt.routing.RoutingTable

data class RoutingTableChanged(val index: Long, val routingTable: RoutingTable)
