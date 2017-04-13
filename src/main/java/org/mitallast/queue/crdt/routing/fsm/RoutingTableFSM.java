package org.mitallast.queue.crdt.routing.fsm;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.mitallast.queue.common.events.EventBus;
import org.mitallast.queue.common.file.FileService;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.crdt.routing.Resource;
import org.mitallast.queue.crdt.routing.RoutingBucket;
import org.mitallast.queue.crdt.routing.RoutingTable;
import org.mitallast.queue.crdt.routing.event.RoutingTableChanged;
import org.mitallast.queue.raft.protocol.RaftSnapshotMetadata;
import org.mitallast.queue.raft.resource.ResourceFSM;
import org.mitallast.queue.raft.resource.ResourceRegistry;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.Optional;

public class RoutingTableFSM implements ResourceFSM {
    private final EventBus eventBus;
    private final StreamService streamService;
    private final File file;

    private volatile long lastApplied;
    private volatile RoutingTable routingTable;

    @Inject
    public RoutingTableFSM(
        Config config,
        EventBus eventBus,
        ResourceRegistry registry,
        FileService fileService,
        StreamService streamService
    ) throws IOException {
        this.eventBus = eventBus;
        this.streamService = streamService;
        this.file = fileService.resource("crdt", "routing.bin");

        this.lastApplied = 0;
        this.routingTable = new RoutingTable(
            config.getInt("crdt.replicas"),
            config.getInt("crdt.buckets")
        );

        restore();

        registry.register(this);
        registry.register(AddResource.class, this::handle);
        registry.register(RemoveResource.class, this::handle);
        registry.register(AddServer.class, this::handle);
        registry.register(RemoveServer.class, this::handle);
        registry.register(Allocate.class, this::handle);
        registry.register(RoutingTable.class, this::handle);
    }

    public RoutingTable get() {
        return routingTable;
    }

    private void restore() throws IOException {
        if (file.length() > 0) {
            try (StreamInput input = streamService.input(file)) {
                lastApplied = input.readLong();
                routingTable = input.readStreamable(RoutingTable::new);
            }
        }
    }

    private void persist(long index, RoutingTable routingTable) throws IOException {
        Preconditions.checkArgument(index > lastApplied);
        RoutingTable prev = this.routingTable;
        this.lastApplied = index;
        this.routingTable = routingTable;
        try (StreamOutput output = streamService.output(file)) {
            output.writeLong(lastApplied);
            output.writeStreamable(routingTable);
        }
        eventBus.trigger(new RoutingTableChanged(prev, routingTable));
    }

    private Streamable handle(long index, RoutingTable routingTable) throws IOException {
        if (index < lastApplied) {
            return null;
        }
        persist(index, routingTable);
        return null;
    }

    private AddResourceResponse handle(long index, AddResource request) throws IOException {
        if (index < lastApplied) {
            return null;
        }
        if (routingTable.hasResource(request.id())) {
            return new AddResourceResponse(request.type(), request.id(), false);
        }
        Resource resource = new Resource(
            request.id(),
            request.type()
        );
        persist(index, routingTable.withResource(resource));
        return new AddResourceResponse(request.type(), request.id(), true);
    }

    private RemoveResourceResponse handle(long index, RemoveResource request) throws IOException {
        if (index < lastApplied) {
            return null;
        }
        if (routingTable.hasResource(request.id())) {
            persist(index, routingTable.withoutResource(request.id()));
            return new RemoveResourceResponse(request.type(), request.id(), true);
        }
        return new RemoveResourceResponse(request.type(), request.id(), false);
    }

    private Streamable handle(long index, AddServer server) throws IOException {
        if (index < lastApplied) {
            return null;
        }
        if (!routingTable.members().contains(server.node())) {
            persist(index, routingTable.withMember(server.node()));
        }
        return null;
    }

    private Streamable handle(long index, RemoveServer server) throws IOException {
        if (index < lastApplied) {
            return null;
        }
        if (!routingTable.members().contains(server.node())) {
            persist(index, routingTable.withoutMember(server.node()));
        }
        return null;
    }

    private Streamable handle(long index, Allocate allocate) throws IOException {
        if (index < lastApplied) {
            return null;
        }
        RoutingBucket bucket = routingTable.bucket(allocate.bucket());
        if (!bucket.members().contains(allocate.node())) {
            persist(index, routingTable.withMember(allocate.bucket(), allocate.node()));
        }
        return null;
    }

    @Override
    public Optional<Streamable> prepareSnapshot(RaftSnapshotMetadata snapshotMeta) {
        return Optional.of(routingTable);
    }
}
