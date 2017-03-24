package org.mitallast.queue.raft.resource;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.protobuf.Message;
import com.typesafe.config.Config;
import org.mitallast.queue.common.Immutable;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.proto.ProtoService;
import org.mitallast.queue.proto.raft.RaftSnapshot;
import org.mitallast.queue.proto.raft.RaftSnapshotMetadata;

import java.io.IOException;

public class ResourceRegistry extends AbstractComponent {

    private final ProtoService protoService;
    private ImmutableList<ResourceFSM> resources;
    private ImmutableMap<Class, ResourceHandler> handlers;

    @Inject
    public ResourceRegistry(Config config, ProtoService protoService) {
        super(config, ResourceRegistry.class);
        this.protoService = protoService;
        this.resources = ImmutableList.of();
        this.handlers = ImmutableMap.of();
    }

    public synchronized void register(ResourceFSM fsm) {
        resources = Immutable.compose(resources, fsm);
    }

    public synchronized <T extends Message> void register(Class<T> type, ResourceHandler<T> handler) {
        handlers = Immutable.compose(handlers, type, handler);
    }

    @SuppressWarnings("unchecked")
    public Message apply(Message message) throws IOException {
        ResourceHandler handler = handlers.get(message.getClass());
        if (handler != null) {
            return handler.apply(message);
        } else {
            logger.warn("resource handler not found: {}", message);
            return null;
        }
    }

    public RaftSnapshot prepareSnapshot(RaftSnapshotMetadata snapshotMeta) {
        RaftSnapshot.Builder builder = RaftSnapshot.newBuilder().setMeta(snapshotMeta);
        for (ResourceFSM resourceFSM : resources) {
            // @todo wrong!
            resourceFSM.prepareSnapshot(snapshotMeta).ifPresent(message -> builder.addData(protoService.pack(message)));
        }
        return builder.build();
    }

    @FunctionalInterface
    public interface ResourceHandler<T> {
        Message apply(T event) throws IOException;
    }
}
