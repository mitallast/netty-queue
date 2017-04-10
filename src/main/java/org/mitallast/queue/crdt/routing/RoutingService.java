package org.mitallast.queue.crdt.routing;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.common.Immutable;
import org.mitallast.queue.common.events.EventBus;
import org.mitallast.queue.crdt.CrdtService;
import org.mitallast.queue.crdt.routing.event.RoutingTableChanged;
import org.mitallast.queue.crdt.routing.fsm.AddServer;
import org.mitallast.queue.crdt.routing.fsm.Allocate;
import org.mitallast.queue.crdt.routing.fsm.RoutingTableFSM;
import org.mitallast.queue.raft.Raft;
import org.mitallast.queue.raft.discovery.ClusterDiscovery;
import org.mitallast.queue.raft.event.ServerAdded;
import org.mitallast.queue.raft.event.ServerRemoved;
import org.mitallast.queue.raft.protocol.ClientMessage;
import org.mitallast.queue.raft.protocol.RemoveServer;
import org.mitallast.queue.transport.DiscoveryNode;

import javax.inject.Inject;
import java.util.concurrent.ThreadLocalRandom;

import static org.mitallast.queue.raft.RaftState.Leader;

public class RoutingService {
    private final static Logger logger = LogManager.getLogger(RoutingService.class);
    private final ClusterDiscovery discovery;
    private final Raft raft;
    private final CrdtService crdtService;
    private final RoutingTableFSM routingTable;

    @Inject
    public RoutingService(ClusterDiscovery discovery, Raft raft, RoutingTableFSM routingTable, EventBus eventBus, CrdtService crdtService) {
        this.discovery = discovery;
        this.raft = raft;
        this.routingTable = routingTable;
        this.crdtService = crdtService;

        eventBus.subscribe(ServerAdded.class, this::handle);
        eventBus.subscribe(ServerRemoved.class, this::handle);
        eventBus.subscribe(RoutingTableChanged.class, this::handle);
    }

    public RoutingTable routingTable() {
        return routingTable.get();
    }

    private void handle(ServerAdded added) {
        if (raft.currentState() == Leader) {
            logger.info("server added");
            raft.apply(new ClientMessage(discovery.self(), new AddServer(added.node())));
        }
    }

    private void handle(ServerRemoved removed) {
        if (raft.currentState() == Leader) {
            logger.info("server removed");
            raft.apply(new ClientMessage(discovery.self(), new RemoveServer(removed.node())));
        }
    }

    private void handle(RoutingTableChanged changed) {
        ImmutableList<DiscoveryNode> members = changed.next().members();
        if (raft.currentState() == Leader) {
            logger.info("routing table changed");
            for (Resource resource : changed.next().resources().values()) {
                if (resource.nodes().size() < resource.replicas() + 1) {
                    ImmutableList<DiscoveryNode> available = Immutable.filterNot(members, member -> resource.nodes().contains(member));
                    if (!available.isEmpty()) {
                        DiscoveryNode node = available.get(ThreadLocalRandom.current().nextInt(available.size()));
                        logger.info("allocate {} {} {}", resource.id(), resource.id(), node);
                        Allocate allocate = new Allocate(resource.id(), node);
                        raft.apply(new ClientMessage(discovery.self(), allocate));
                        return;
                    }
                }
            }
        }
        for (Resource resource : changed.next().resources().values()) {
            if (resource.nodes().contains(discovery.self())) {
                switch (resource.type()) {
                    case LWWRegister:
                        crdtService.createLWWRegister(resource.id());
                        break;
                    default:
                        logger.warn("unexpected type: {}", resource.type());
                }
            }
        }
    }
}
