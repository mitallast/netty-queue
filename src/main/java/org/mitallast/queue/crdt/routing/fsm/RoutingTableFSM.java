package org.mitallast.queue.crdt.routing.fsm;

import com.typesafe.config.Config;
import org.mitallast.queue.common.events.EventBus;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.crdt.routing.Resource;
import org.mitallast.queue.crdt.routing.RoutingBucket;
import org.mitallast.queue.crdt.routing.RoutingTable;
import org.mitallast.queue.crdt.routing.event.RoutingTableChanged;
import org.mitallast.queue.raft.protocol.RaftSnapshotMetadata;
import org.mitallast.queue.raft.resource.ResourceFSM;
import org.mitallast.queue.raft.resource.ResourceRegistry;

import javax.inject.Inject;
import java.util.Optional;

public class RoutingTableFSM implements ResourceFSM {
    private final EventBus eventBus;

    private volatile RoutingTable routingTable;

    @Inject
    public RoutingTableFSM(Config config, EventBus eventBus, ResourceRegistry registry) {
        this.routingTable = new RoutingTable(
            config.getInt("crdt.replicas"),
            config.getInt("crdt.buckets")
        );
        this.eventBus = eventBus;
        registry.register(this);
        registry.register(AddResource.class, this::handle);
        registry.register(RemoveResource.class, this::handle);
        registry.register(AddServer.class, this::handle);
        registry.register(RemoveServer.class, this::handle);
        registry.register(Allocate.class, this::handle);
        registry.register(RoutingTable.class, this::update);
    }

    private Streamable update(RoutingTable routingTable) {
        RoutingTable prev = this.routingTable;
        this.routingTable = routingTable;
        eventBus.trigger(new RoutingTableChanged(prev, routingTable));
        return null;
    }

    public RoutingTable get() {
        return routingTable;
    }

    private AddResourceResponse handle(AddResource request) {
        if (routingTable.hasResource(request.id())) {
            return new AddResourceResponse(request.type(), request.id(), false);
        }
        Resource resource = new Resource(
            request.id(),
            request.type()
        );
        update(routingTable.withResource(resource));
        return new AddResourceResponse(request.type(), request.id(), true);
    }

    private RemoveResourceResponse handle(RemoveResource request) {
        if (routingTable.hasResource(request.id())) {
            update(routingTable.withoutResource(request.id()));
            return new RemoveResourceResponse(request.type(), request.id(), true);
        }
        return new RemoveResourceResponse(request.type(), request.id(), false);
    }

    private Streamable handle(AddServer server) {
        if (!routingTable.members().contains(server.node())) {
            update(routingTable.withMember(server.node()));
        }
        return null;
    }

    private Streamable handle(RemoveServer server) {
        if (!routingTable.members().contains(server.node())) {
            update(routingTable.withoutMember(server.node()));
        }
        return null;
    }

    private Streamable handle(Allocate allocate) {
        RoutingBucket bucket = routingTable.bucket(allocate.bucket());
        if (!bucket.members().contains(allocate.node())) {
            update(routingTable.withMember(allocate.bucket(), allocate.node()));
        }

        return null;
    }

    @Override
    public Optional<Streamable> prepareSnapshot(RaftSnapshotMetadata snapshotMeta) {
        return Optional.of(routingTable);
    }
}
