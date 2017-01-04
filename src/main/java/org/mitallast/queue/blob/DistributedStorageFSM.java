package org.mitallast.queue.blob;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.mitallast.queue.blob.protocol.BlobRoutingMap;
import org.mitallast.queue.blob.protocol.PutBlobResource;
import org.mitallast.queue.blob.protocol.PutBlobResourceResponse;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.resource.ResourceFSM;
import org.mitallast.queue.raft.protocol.RaftSnapshot;
import org.mitallast.queue.raft.protocol.RaftSnapshotMetadata;
import org.mitallast.queue.raft.resource.ResourceRegistry;

import java.util.Optional;

public class DistributedStorageFSM extends AbstractComponent implements ResourceFSM {

    private volatile BlobRoutingMap routingMap = new BlobRoutingMap(ImmutableMap.of());

    @Inject
    public DistributedStorageFSM(Config config, ResourceRegistry registry) {
        super(config, DistributedStorageFSM.class);
        registry.register(this);
        registry.register(PutBlobResource.class, this::handle);
        registry.register(BlobRoutingMap.class, this::handle);
    }

    public BlobRoutingMap getRoutingMap() {
        return routingMap;
    }

    public Streamable handle(PutBlobResource resource) {
        logger.info("put resource to routing map: {} ", resource);
        routingMap = routingMap.withResource(resource.getKey(), resource.getNode());
        return new PutBlobResourceResponse(
            resource.getId(),
            resource.getKey(),
            true
        );
    }

    public Streamable handle(BlobRoutingMap map) {
        logger.info("install routing map: {}", routingMap);
        routingMap = map;
        return null;
    }

    @Override
    public Optional<Streamable> prepareSnapshot(RaftSnapshotMetadata snapshotMeta) {
        return Optional.of(routingMap);
    }
}
