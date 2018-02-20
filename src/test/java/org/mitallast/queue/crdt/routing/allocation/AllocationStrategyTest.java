package org.mitallast.queue.crdt.routing.allocation;

import io.vavr.collection.HashSet;
import io.vavr.control.Option;
import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.crdt.routing.RoutingReplica;
import org.mitallast.queue.crdt.routing.RoutingTable;
import org.mitallast.queue.crdt.routing.fsm.AddReplica;
import org.mitallast.queue.crdt.routing.fsm.CloseReplica;
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
        expected(new AddReplica(0, node1), strategy.update(table3.withMembers(HashSet.of(node1))));
    }

    @Test
    public void testAllocate_node1_1() {
        RoutingTable updated = table3.withMembers(HashSet.of(node1))
            .withReplica(0, node1);
        expected(new AddReplica(1, node1), strategy.update(updated));
    }

    @Test
    public void testAllocate_node1_2() {
        RoutingTable updated = table3.withMembers(HashSet.of(node1))
            .withReplica(0, node1)
            .withReplica(1, node1);
        expected(new AddReplica(2, node1), strategy.update(updated));
    }

    @Test
    public void testAllocate_node1_3() {
        RoutingTable updated = table3.withMembers(HashSet.of(node1))
            .withReplica(0, node1)
            .withReplica(1, node1)
            .withReplica(2, node1);
        expectedNone(strategy.update(updated));
    }

    @Test
    public void testAllocate_node1_0_node2_0() {
        RoutingTable updated = table3.withMembers(HashSet.of(node1, node2));
        expected(new AddReplica(0, node1), strategy.update(updated));
    }

    @Test
    public void testAllocate_node1_1_node2_0() {
        RoutingTable updated = table3.withMembers(HashSet.of(node1, node2))
            .withReplica(0, node1);
        expected(new AddReplica(1, node2), strategy.update(updated));
    }

    @Test
    public void testAllocate_node1_2_node2_0() {
        RoutingTable updated = table3.withMembers(HashSet.of(node1, node2))
            .withReplica(0, node1)
            .withReplica(1, node1);
        expected(new AddReplica(2, node2), strategy.update(updated));
    }

    @Test
    public void testAllocate_node1_3_node2_0() {
        RoutingTable updated = table3.withMembers(HashSet.of(node1, node2))
            .withReplica(0, node1)
            .withReplica(1, node1)
            .withReplica(2, node1);
        expected(new CloseReplica(0, 0), strategy.update(updated));
    }

    @Test
    public void testAllocate_node1_2_node2_1() {
        RoutingTable updated = table3.withMembers(HashSet.of(node1, node2))
            .withReplica(0, node1)
            .withReplica(1, node1)
            .withReplica(2, node2);
        expectedNone(strategy.update(updated));
    }

    @Test
    public void testAllocate_node1_11_node2_0_node3_0() {
        RoutingTable updated = table11.withMembers(HashSet.of(node1, node2, node3))
            .withReplica(0, node1)
            .withReplica(1, node1)
            .withReplica(2, node1)
            .withReplica(3, node1)
            .withReplica(4, node1)
            .withReplica(5, node1)
            .withReplica(6, node1)
            .withReplica(7, node1)
            .withReplica(8, node1)
            .withReplica(9, node1)
            .withReplica(10, node1);
        expected(new CloseReplica(0, 0), strategy.update(updated));
    }

    @Test
    public void testAllocate_node1_6_node2_5_node3_0() {
        RoutingTable updated = table11.withMembers(HashSet.of(node1, node2, node3))
            .withReplica(0, node1)
            .withReplica(1, node1)
            .withReplica(2, node1)
            .withReplica(3, node1)
            .withReplica(4, node1)
            .withReplica(5, node1)
            .withReplica(6, node2)
            .withReplica(7, node2)
            .withReplica(8, node2)
            .withReplica(9, node2)
            .withReplica(10, node2);
        expected(new CloseReplica(0, 0), strategy.update(updated));
    }

    @Test
    public void testAllocate_node1_5_node2_5_node3_1() {
        RoutingTable updated = table11.withMembers(HashSet.of(node1, node2, node3))
            .withReplica(0, node3)
            .withReplica(1, node1)
            .withReplica(2, node1)
            .withReplica(3, node1)
            .withReplica(4, node1)
            .withReplica(5, node1)
            .withReplica(6, node2)
            .withReplica(7, node2)
            .withReplica(8, node2)
            .withReplica(9, node2)
            .withReplica(10, node2);
        expected(new CloseReplica(1, 1), strategy.update(updated));
    }

    @Test
    public void testAllocate_node1_4_node2_5_node3_2() {
        RoutingTable updated = table11.withMembers(HashSet.of(node1, node2, node3))
            .withReplica(0, node3)
            .withReplica(1, node3)
            .withReplica(2, node1)
            .withReplica(3, node1)
            .withReplica(4, node1)
            .withReplica(5, node1)
            .withReplica(6, node2)
            .withReplica(7, node2)
            .withReplica(8, node2)
            .withReplica(9, node2)
            .withReplica(10, node2);
        expected(new CloseReplica(6, 6), strategy.update(updated));
    }

    @Test
    public void testAllocate_node1_4_node2_4_node3_3() {
        RoutingTable updated = table11.withMembers(HashSet.of(node1, node2, node3))
            .withReplica(0, node3)
            .withReplica(1, node3)
            .withReplica(2, node1)
            .withReplica(3, node1)
            .withReplica(4, node1)
            .withReplica(5, node1)
            .withReplica(6, node3)
            .withReplica(7, node2)
            .withReplica(8, node2)
            .withReplica(9, node2)
            .withReplica(10, node2);
        expectedNone(strategy.update(updated));
    }

    @Test
    public void testReplica_node1_0_node2_0_node3_0() {
        RoutingTable updated = new RoutingTable(3, 1)
            .withMembers(HashSet.of(node1, node2, node3));

        expected(new AddReplica(0, node1), strategy.update(updated));
    }

    @Test
    public void testReplica_node1_1_node2_0_node3_0() {
        RoutingTable updated = new RoutingTable(3, 1)
            .withMembers(HashSet.of(node1, node2, node3))
            .withReplica(0, node1);

        expected(new AddReplica(0, node2), strategy.update(updated));
    }

    @Test
    public void testReplica_node1_1_node2_1_node3_0() {
        RoutingTable updated = new RoutingTable(3, 1)
            .withMembers(HashSet.of(node1, node2, node3))
            .withReplica(0, node1)
            .withReplica(0, node2);

        expected(new AddReplica(0, node3), strategy.update(updated));
    }

    @Test
    public void testReplica_node1_1_node2_1_node3_1() {
        RoutingTable updated = new RoutingTable(3, 1)
            .withMembers(HashSet.of(node1, node2, node3))
            .withReplica(0, node1)
            .withReplica(0, node2)
            .withReplica(0, node3);

        expectedNone(strategy.update(updated));
    }

    @Test
    public void testReplica_node1_0() {
        RoutingTable updated = new RoutingTable(1, 3)
            .withMembers(HashSet.of(node1));

        expected(new AddReplica(0, node1), strategy.update(updated));
    }

    @Test
    public void testReplica_node1_2_closed_1_node2_0() {
        RoutingTable updated = new RoutingTable(1, 3)
            .withMembers(HashSet.of(node1, node2))
            .withReplica(0, new RoutingReplica(0, node1).close())
            .withReplica(1, node1)
            .withReplica(2, node1);

        expected(new AddReplica(0, node2), strategy.update(updated));
    }

    private void expectedNone(Option<Message> actual) {
        Assert.assertEquals(Option.none(), actual);
    }

    private void expected(Object expected, Option<Message> actual) {
        Assert.assertEquals(Option.of(expected), actual);
    }
}
