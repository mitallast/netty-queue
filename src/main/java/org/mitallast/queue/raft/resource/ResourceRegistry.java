package org.mitallast.queue.raft.resource;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.common.Immutable;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.protocol.RaftSnapshot;
import org.mitallast.queue.raft.protocol.RaftSnapshotMetadata;

import java.io.IOException;

public class ResourceRegistry {
    private final static Logger logger = LogManager.getLogger();

    private ImmutableList<ResourceFSM> resources = ImmutableList.of();
    private ImmutableMap<Class, ResourceHandler> handlers = ImmutableMap.of();

    public synchronized void register(ResourceFSM fsm) {
        resources = Immutable.compose(resources, fsm);
    }

    public synchronized <T extends Streamable> void register(Class<T> type, ResourceHandler<T> handler) {
        handlers = Immutable.compose(handlers, type, handler);
    }

    @SuppressWarnings("unchecked")
    public Streamable apply(Streamable event) throws IOException {
        ResourceHandler handler = handlers.get(event.getClass());
        if (handler != null) {
            return handler.apply(event);
        } else {
            logger.warn("resource handler not found: {}", event);
            return null;
        }
    }

    public RaftSnapshot prepareSnapshot(RaftSnapshotMetadata snapshotMeta) {
        ImmutableList.Builder<Streamable> builder = ImmutableList.builder();
        for (ResourceFSM resourceFSM : resources) {
            resourceFSM.prepareSnapshot(snapshotMeta).ifPresent(builder::add);
        }
        return new RaftSnapshot(snapshotMeta, builder.build());
    }

    @FunctionalInterface
    public interface ResourceHandler<T> {
        Streamable apply(T event) throws IOException;
    }
}
