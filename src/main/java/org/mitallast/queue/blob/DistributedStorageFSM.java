package org.mitallast.queue.blob;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.mitallast.queue.blob.protocol.BlobRoutingMap;
import org.mitallast.queue.blob.protocol.PutBlobResource;
import org.mitallast.queue.blob.protocol.PutBlobResourceResponse;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.ResourceFSM;
import org.mitallast.queue.raft.protocol.RaftSnapshot;
import org.mitallast.queue.raft.protocol.RaftSnapshotMetadata;

import java.util.Optional;

public class DistributedStorageFSM extends AbstractComponent implements ResourceFSM {

    private volatile BlobRoutingMap routingMap = new BlobRoutingMap(ImmutableMap.of());

    @Inject
    public DistributedStorageFSM(Config config) {
        super(config, DistributedStorageFSM.class);
    }

    public BlobRoutingMap getRoutingMap() {
        return routingMap;
    }

    @Override
    public Streamable apply(Streamable message) {
        if (message instanceof PutBlobResource) {
            PutBlobResource resource = (PutBlobResource) message;
            logger.info("put resource to routing map: {} ", resource);
            routingMap = routingMap.withResource(resource.getKey(), resource.getNode());
            return new PutBlobResourceResponse(
                resource.getId(),
                resource.getKey(),
                true
            );
        } else if (message instanceof BlobRoutingMap) {
            logger.info("install routing map: {}", routingMap);
            routingMap = (BlobRoutingMap) message;
        }
        return null;
    }

    @Override
    public Optional<RaftSnapshot> prepareSnapshot(RaftSnapshotMetadata snapshotMeta) {
        return Optional.of(new RaftSnapshot(snapshotMeta, routingMap));
    }
}
