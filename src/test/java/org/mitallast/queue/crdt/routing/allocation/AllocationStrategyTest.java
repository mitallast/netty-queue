package org.mitallast.queue.crdt.routing.allocation;

import javaslang.collection.HashSet;
import javaslang.control.Option;
import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.crdt.routing.RoutingTable;
import org.mitallast.queue.crdt.routing.fsm.AddBucketMember;
import org.mitallast.queue.crdt.routing.fsm.CloseBucketMember;
import org.mitallast.queue.transport.DiscoveryNode;

public class AllocationStrategyTest extends BaseTest {

    private final AllocationStrategy strategy = new DefaultAllocationStrategy();
    private final DiscoveryNode node1 = new DiscoveryNode("localhost", 8801);
    private final DiscoveryNode node2 = new DiscoveryNode("localhost", 8802);
    private final DiscoveryNode node3 = new DiscoveryNode("localhost", 8803);

    private final RoutingTable table3 = new RoutingTable(1, 3);
    private final RoutingTable table11 = new RoutingTable(1, 11);

    @Test
    public void testEmpty() {
        expectedNone(strategy.update(table3));
    }

    @Test
    public void testAllocate_node1_0() {
        expected(new AddBucketMember(0, node1), strategy.update(table3.withMembers(HashSet.of(node1))));
    }

    @Test
    public void testAllocate_node1_1() {
        RoutingTable updated = table3.withMembers(HashSet.of(node1))
            .withBucketMember(0, node1);
        expected(new AddBucketMember(1, node1), strategy.update(updated));
    }

    @Test
    public void testAllocate_node1_2() {
        RoutingTable updated = table3.withMembers(HashSet.of(node1))
            .withBucketMember(0, node1)
            .withBucketMember(1, node1);
        expected(new AddBucketMember(2, node1), strategy.update(updated));
    }

    @Test
    public void testAllocate_node1_3() {
        RoutingTable updated = table3.withMembers(HashSet.of(node1))
            .withBucketMember(0, node1)
            .withBucketMember(1, node1)
            .withBucketMember(2, node1);
        expectedNone(strategy.update(updated));
    }

    @Test
    public void testAllocate_node1_0_node2_0() {
        RoutingTable updated = table3.withMembers(HashSet.of(node1, node2));
        expected(new AddBucketMember(0, node1), strategy.update(updated));
    }

    @Test
    public void testAllocate_node1_1_node2_0() {
        RoutingTable updated = table3.withMembers(HashSet.of(node1, node2))
            .withBucketMember(0, node1);
        expected(new AddBucketMember(1, node2), strategy.update(updated));
    }

    @Test
    public void testAllocate_node1_2_node2_0() {
        RoutingTable updated = table3.withMembers(HashSet.of(node1, node2))
            .withBucketMember(0, node1)
            .withBucketMember(1, node1);
        expected(new AddBucketMember(2, node2), strategy.update(updated));
    }

    @Test
    public void testAllocate_node1_3_node2_0() {
        RoutingTable updated = table3.withMembers(HashSet.of(node1, node2))
            .withBucketMember(0, node1)
            .withBucketMember(1, node1)
            .withBucketMember(2, node1);
        expected(new CloseBucketMember(0, node1), strategy.update(updated));
    }

    @Test
    public void testAllocate_node1_2_node2_1() {
        RoutingTable updated = table3.withMembers(HashSet.of(node1, node2))
            .withBucketMember(0, node1)
            .withBucketMember(1, node1)
            .withBucketMember(2, node2);
        expectedNone(strategy.update(updated));
    }

    @Test
    public void testAllocate_node1_11_node2_0_node3_0() {
        RoutingTable updated = table11.withMembers(HashSet.of(node1, node2, node3))
            .withBucketMember(0, node1)
            .withBucketMember(1, node1)
            .withBucketMember(2, node1)
            .withBucketMember(3, node1)
            .withBucketMember(4, node1)
            .withBucketMember(5, node1)
            .withBucketMember(6, node1)
            .withBucketMember(7, node1)
            .withBucketMember(8, node1)
            .withBucketMember(9, node1)
            .withBucketMember(10, node1);
        expected(new CloseBucketMember(0, node1), strategy.update(updated));
    }

    @Test
    public void testAllocate_node1_6_node2_5_node3_0() {
        RoutingTable updated = table11.withMembers(HashSet.of(node1, node2, node3))
            .withBucketMember(0, node1)
            .withBucketMember(1, node1)
            .withBucketMember(2, node1)
            .withBucketMember(3, node1)
            .withBucketMember(4, node1)
            .withBucketMember(5, node1)
            .withBucketMember(6, node2)
            .withBucketMember(7, node2)
            .withBucketMember(8, node2)
            .withBucketMember(9, node2)
            .withBucketMember(10, node2);
        expected(new CloseBucketMember(0, node1), strategy.update(updated));
    }

    @Test
    public void testAllocate_node1_5_node2_5_node3_1() {
        RoutingTable updated = table11.withMembers(HashSet.of(node1, node2, node3))
            .withBucketMember(0, node3)
            .withBucketMember(1, node1)
            .withBucketMember(2, node1)
            .withBucketMember(3, node1)
            .withBucketMember(4, node1)
            .withBucketMember(5, node1)
            .withBucketMember(6, node2)
            .withBucketMember(7, node2)
            .withBucketMember(8, node2)
            .withBucketMember(9, node2)
            .withBucketMember(10, node2);
        expected(new CloseBucketMember(1, node1), strategy.update(updated));
    }

    @Test
    public void testAllocate_node1_4_node2_5_node3_2() {
        RoutingTable updated = table11.withMembers(HashSet.of(node1, node2, node3))
            .withBucketMember(0, node3)
            .withBucketMember(1, node3)
            .withBucketMember(2, node1)
            .withBucketMember(3, node1)
            .withBucketMember(4, node1)
            .withBucketMember(5, node1)
            .withBucketMember(6, node2)
            .withBucketMember(7, node2)
            .withBucketMember(8, node2)
            .withBucketMember(9, node2)
            .withBucketMember(10, node2);
        expected(new CloseBucketMember(6, node2), strategy.update(updated));
    }

    @Test
    public void testAllocate_node1_4_node2_4_node3_3() {
        RoutingTable updated = table11.withMembers(HashSet.of(node1, node2, node3))
            .withBucketMember(0, node3)
            .withBucketMember(1, node3)
            .withBucketMember(2, node1)
            .withBucketMember(3, node1)
            .withBucketMember(4, node1)
            .withBucketMember(5, node1)
            .withBucketMember(6, node3)
            .withBucketMember(7, node2)
            .withBucketMember(8, node2)
            .withBucketMember(9, node2)
            .withBucketMember(10, node2);
        expectedNone(strategy.update(updated));
    }

    private void expectedNone(Option<Streamable> actual) {
        Assert.assertEquals(Option.none(), actual);
    }

    private void expected(Streamable expected, Option<Streamable> actual) {
        Assert.assertEquals(Option.of(expected), actual);
    }
}
