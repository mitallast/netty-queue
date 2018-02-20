package org.mitallast.queue.crdt.routing.allocation

import io.vavr.control.Option
import org.apache.logging.log4j.LogManager
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.crdt.routing.RoutingTable
import org.mitallast.queue.crdt.routing.fsm.AddReplica
import org.mitallast.queue.crdt.routing.fsm.CloseReplica
import org.mitallast.queue.transport.DiscoveryNode

class DefaultAllocationStrategy : AllocationStrategy {

    override fun update(routingTable: RoutingTable): Option<Message> {
        val members = routingTable.members

        val stats = routingTable.buckets
            .flatMap { it.replicas.values() }
            .groupBy<DiscoveryNode> { it.member }

        var rebalance = false
        for (routingBucket in routingTable.buckets) {
            // check for new allocations
            val open = routingBucket.replicas.values().count { it.isOpened }
            if (open < routingTable.replicas) {
                logger.info("bucket {} has open {} < {} replicas", routingBucket.index, open, routingTable.replicas)
                val bucketMembers = routingBucket.replicas.values().map<DiscoveryNode> { it.member }.toSet()
                val available = members.diff(bucketMembers).minBy { a, b ->
                    val sizeA = stats.get(a).map { it.size() }.getOrElse(0)
                    val sizeB = stats.get(b).map { it.size() }.getOrElse(0)
                    Integer.compare(sizeA, sizeB)
                }
                if (!available.isEmpty) {
                    val node = available.get()
                    logger.info("add replica bucket {} {}", routingBucket.index, node)
                    val request = AddReplica(routingBucket.index, node)
                    return Option.some(request)
                } else {
                    logger.warn("no available nodes")
                }
            }
            if (routingBucket.replicas.values().exists { it.isClosed }) {
                rebalance = true
            }
        }
        if (!rebalance && members.nonEmpty()) {
            val minimumBuckets = routingTable.buckets.size() / members.size()
            for (m in members) {
                val count = routingTable.bucketsCount(m)
                if (routingTable.bucketsCount(m) < minimumBuckets) {
                    logger.warn("node {} buckets {} < min {}", m, count, minimumBuckets)
                    val memberBuckets = routingTable.buckets.filter { it.exists(m) }

                    val availableBuckets = routingTable.buckets
                        .removeAll(memberBuckets)
                        .filter { bucket ->
                            bucket.replicas
                                .values()
                                .filter { it.member != m } // without current node
                                .exists { rm -> routingTable.bucketsCount(rm.member) > minimumBuckets }
                        }

                    val available = members.filter { t -> availableBuckets.exists { b -> b.exists(t) } }
                        .maxBy { a, b ->
                            val sizeA = stats.get(a).map { it.size() }.getOrElse(0)
                            val sizeB = stats.get(b).map { it.size() }.getOrElse(0)
                            Integer.compare(sizeA, sizeB)
                        }

                    if (available.isDefined) {
                        val last = available.get()
                        val closeOpt = availableBuckets.find { bucket -> bucket.exists(last) }
                        if (closeOpt.isDefined) {
                            val close = closeOpt.get()
                            val replica = close.replica(last).get()
                            logger.info("close replica bucket {} {}", close.index, replica.id)
                            val request = CloseReplica(close.index, replica.id)
                            return Option.some(request)
                        } else {
                            logger.warn("no bucket to close found")
                        }
                    } else {
                        logger.warn("no available buckets")
                    }
                }
            }
        }
        return Option.none()
    }

    companion object {
        private val logger = LogManager.getLogger(AllocationStrategy::class.java)
    }
}
