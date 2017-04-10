package org.mitallast.queue.crdt.routing.fsm;

import com.google.common.collect.ImmutableSet;
import org.mitallast.queue.common.events.EventBus;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.crdt.routing.Resource;
import org.mitallast.queue.crdt.routing.RoutingTable;
import org.mitallast.queue.crdt.routing.event.RoutingTableChanged;
import org.mitallast.queue.raft.protocol.RaftSnapshotMetadata;
import org.mitallast.queue.raft.resource.ResourceFSM;
import org.mitallast.queue.raft.resource.ResourceRegistry;

import javax.inject.Inject;
import java.util.Optional;

public class RoutingTableFSM implements ResourceFSM {
    private final EventBus eventBus;

    private volatile RoutingTable routingTable = new RoutingTable();

    @Inject
    public RoutingTableFSM(EventBus eventBus, ResourceRegistry registry) {
        this.eventBus = eventBus;
        registry.register(this);
        registry.register(AddResource.class, this::handle);
        registry.register(RemoveResource.class, this::handle);
        registry.register(AddServer.class, this::handle);
        registry.register(RemoveServer.class, this::handle);
        registry.register(Allocate.class, this::handle);
    }

    public RoutingTable get() {
        return routingTable;
    }

    private AddResourceResponse handle(AddResource request) {
        if (routingTable.resources().containsKey(request.id())) {
            return new AddResourceResponse(request.type(), request.id(), false);
        }
        Resource resource = new Resource(
            request.type(),
            request.id(),
            request.replicas(),
            ImmutableSet.of()
        );
        RoutingTable prev = routingTable;
        routingTable = routingTable.withResource(resource);
        eventBus.trigger(new RoutingTableChanged(prev, routingTable));
        return new AddResourceResponse(request.type(), request.id(), true);
    }

    private RemoveResourceResponse handle(RemoveResource request) {
        if (routingTable.resources().containsKey(request.id())) {
            RoutingTable prev = routingTable;
            routingTable = routingTable.withoutResource(request.id());
            eventBus.trigger(new RoutingTableChanged(prev, routingTable));
            return new RemoveResourceResponse(request.type(), request.id(), true);
        }
        return new RemoveResourceResponse(request.type(), request.id(), false);
    }

    private Streamable handle(AddServer server) {
        if (!routingTable.members().contains(server.node())) {
            RoutingTable prev = routingTable;
            routingTable = routingTable.withMember(server.node());
            eventBus.trigger(new RoutingTableChanged(prev, routingTable));
        }
        return null;
    }

    private Streamable handle(RemoveServer server) {
        if (!routingTable.members().contains(server.node())) {
            RoutingTable prev = routingTable;
            routingTable = routingTable.withoutMember(server.node());
            eventBus.trigger(new RoutingTableChanged(prev, routingTable));
        }
        return null;
    }

    private Streamable handle(Allocate allocate) {
        Resource resource = routingTable.resources().get(allocate.resource());
        if (!resource.nodes().contains(allocate.node())) {
            RoutingTable prev = routingTable;
            routingTable = routingTable.withResource(resource.withMember(allocate.node()));
            eventBus.trigger(new RoutingTableChanged(prev, routingTable));
        }
        return null;
    }

    @Override
    public Optional<Streamable> prepareSnapshot(RaftSnapshotMetadata snapshotMeta) {
        return Optional.of(routingTable);
    }
}
