package org.mitallast.queue.crdt.routing.allocation;

import javaslang.collection.Map;
import javaslang.collection.Set;
import javaslang.collection.Vector;
import javaslang.control.Option;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.crdt.routing.RoutingBucket;
import org.mitallast.queue.crdt.routing.RoutingReplica;
import org.mitallast.queue.crdt.routing.RoutingTable;
import org.mitallast.queue.crdt.routing.fsm.AddReplica;
import org.mitallast.queue.crdt.routing.fsm.CloseReplica;
import org.mitallast.queue.transport.DiscoveryNode;

public class DefaultAllocationStrategy implements AllocationStrategy {
    private final static Logger logger = LogManager.getLogger(AllocationStrategy.class);

    @Override
    public Option<Message> update(RoutingTable routingTable) {
        Set<DiscoveryNode> members = routingTable.members();

        Map<DiscoveryNode, Vector<RoutingReplica>> stats = routingTable.buckets()
            .flatMap(bucket -> bucket.replicas().values())
            .groupBy(RoutingReplica::member);

        boolean rebalance = false;
        for (RoutingBucket routingBucket : routingTable.buckets()) {
            // check for new allocations
            int open = routingBucket.replicas().values().count(RoutingReplica::isOpened);
            if (open < routingTable.replicas()) {
                logger.info("bucket {} has open {} < {} replicas", routingBucket.index(), open, routingTable.replicas());
                Set<DiscoveryNode> bucketMembers = routingBucket.replicas().values().map(RoutingReplica::member).toSet();
                Option<DiscoveryNode> available = members.diff(bucketMembers).minBy((a, b) -> {
                    int sizeA = stats.get(a).map(Vector::size).getOrElse(0);
                    int sizeB = stats.get(b).map(Vector::size).getOrElse(0);
                    return Integer.compare(sizeA, sizeB);
                });
                if (!available.isEmpty()) {
                    DiscoveryNode node = available.get();
                    logger.info("add replica bucket {} {}", routingBucket.index(), node);
                    AddReplica request = new AddReplica(routingBucket.index(), node);
                    return Option.some(request);
                } else {
                    logger.warn("no available nodes");
                }
            }
            if (routingBucket.replicas().values().exists(RoutingReplica::isClosed)) {
                rebalance = true;
            }
        }
        if (!rebalance && members.nonEmpty()) {
            int minimumBuckets = routingTable.buckets().size() / members.size();
            for (DiscoveryNode m : members) {
                final DiscoveryNode member = m;
                int count = routingTable.bucketsCount(member);
                if (routingTable.bucketsCount(member) < minimumBuckets) {
                    logger.warn("node {} buckets {} < min {}", member, count, minimumBuckets);
                    Vector<RoutingBucket> memberBuckets = routingTable.buckets()
                        .filter(bucket -> bucket.exists(member));

                    Vector<RoutingBucket> availableBuckets = routingTable.buckets()
                        .removeAll(memberBuckets)
                        .filter(bucket -> bucket.replicas()
                            .values()
                            .filter(replica -> !replica.member().equals(member)) // without current node
                            .exists(rm -> routingTable.bucketsCount(rm.member()) > minimumBuckets) // do not include nodes with min buckets
                        );

                    Option<DiscoveryNode> available = members.filter(t -> availableBuckets.exists(b -> b.exists(t)))
                        .maxBy((a, b) -> {
                            int sizeA = stats.get(a).map(Vector::size).getOrElse(0);
                            int sizeB = stats.get(b).map(Vector::size).getOrElse(0);
                            return Integer.compare(sizeA, sizeB);
                        });

                    if (available.isDefined()) {
                        DiscoveryNode last = available.get();
                        Option<RoutingBucket> closeOpt = availableBuckets.find(bucket -> bucket.exists(last));
                        if (closeOpt.isDefined()) {
                            RoutingBucket close = closeOpt.get();
                            RoutingReplica replica = close.replica(last).get();
                            logger.info("close replica bucket {} {}", close.index(), replica.id());
                            CloseReplica request = new CloseReplica(close.index(), replica.id());
                            return Option.some(request);
                        } else {
                            logger.warn("no bucket to close found");
                        }
                    } else {
                        logger.warn("no available buckets");
                    }
                }
            }
        }
        return Option.none();
    }
}
