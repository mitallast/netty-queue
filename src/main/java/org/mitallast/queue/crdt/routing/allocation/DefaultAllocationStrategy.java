package org.mitallast.queue.crdt.routing.allocation;

import javaslang.collection.Map;
import javaslang.collection.Set;
import javaslang.collection.Vector;
import javaslang.control.Option;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.crdt.routing.BucketMember;
import org.mitallast.queue.crdt.routing.RoutingBucket;
import org.mitallast.queue.crdt.routing.RoutingTable;
import org.mitallast.queue.crdt.routing.fsm.AddBucketMember;
import org.mitallast.queue.crdt.routing.fsm.CloseBucketMember;
import org.mitallast.queue.transport.DiscoveryNode;

public class DefaultAllocationStrategy implements AllocationStrategy {
    private final Logger logger = LogManager.getLogger();

    @Override
    public Option<Streamable> update(RoutingTable routingTable) {
        Set<DiscoveryNode> members = routingTable.members();

        Map<DiscoveryNode, Vector<BucketMember>> stats = routingTable.buckets()
            .flatMap(bucket -> bucket.members().values())
            .groupBy(BucketMember::member);

        boolean rebalance = false;
        for (RoutingBucket routingBucket : routingTable.buckets()) {
            // check for new allocations
            int open = routingBucket.members().values().count(BucketMember::isOpened);
            if (open < routingTable.replicas()) {
                logger.info("bucket {} has open {} < {} replicas", routingBucket.index(), open, routingTable.replicas());
                Set<DiscoveryNode> bucketMembers = routingBucket.members().keySet();
                Option<DiscoveryNode> available = members.diff(bucketMembers).minBy((a, b) -> {
                    int sizeA = stats.get(a).map(Vector::size).getOrElse(0);
                    int sizeB = stats.get(b).map(Vector::size).getOrElse(0);
                    return Integer.compare(sizeA, sizeB);
                });
                if (!available.isEmpty()) {
                    DiscoveryNode node = available.get();
                    logger.info("addBucketMember bucket {} {}", routingBucket.index(), node);
                    AddBucketMember request = new AddBucketMember(routingBucket.index(), node);
                    return Option.some(request);
                } else {
                    logger.warn("no available nodes");
                }
            }
            if (routingBucket.members().values().exists(BucketMember::isClosed)) {
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
                        .filter(bucket -> bucket.members().containsKey(member));

                    Vector<RoutingBucket> availableBuckets = routingTable.buckets()
                        .removeAll(memberBuckets)
                        .filter(bucket -> bucket.members()
                            .remove(member) // without current node
                            .values()
                            .exists(rm -> routingTable.bucketsCount(rm.member()) > minimumBuckets) // do not include nodes with min buckets
                        );

                    Option<DiscoveryNode> available = members.filter(t -> availableBuckets.exists(b -> b.members().containsKey(t)))
                        .maxBy((a, b) -> {
                            int sizeA = stats.get(a).map(Vector::size).getOrElse(0);
                            int sizeB = stats.get(b).map(Vector::size).getOrElse(0);
                            return Integer.compare(sizeA, sizeB);
                        });

                    if (available.isDefined()) {
                        DiscoveryNode last = available.get();
                        RoutingBucket close = routingTable.buckets()
                            .find(bucket -> bucket.members().containsKey(last))
                            .get();
                        logger.info("CloseBucketMember bucket {} {}", close.index(), last);
                        CloseBucketMember request = new CloseBucketMember(close.index(), last);
                        return Option.some(request);
                    }
                }
            }
        }
        return Option.none();
    }
}
