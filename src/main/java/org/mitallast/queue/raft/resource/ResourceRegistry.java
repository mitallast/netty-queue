package org.mitallast.queue.raft.resource;

import javaslang.collection.HashMap;
import javaslang.collection.Map;
import javaslang.collection.Vector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.protocol.RaftSnapshot;
import org.mitallast.queue.raft.protocol.RaftSnapshotMetadata;

public class ResourceRegistry {
    private final static Logger logger = LogManager.getLogger();

    private Vector<ResourceFSM> resources = Vector.empty();
    private Map<Class, ResourceHandler> handlers = HashMap.empty();

    public synchronized void register(ResourceFSM fsm) {
        resources = resources.append(fsm);
    }

    public synchronized <T extends Streamable> void register(Class<T> type, ResourceHandler<T> handler) {
        handlers = handlers.put(type, handler);
    }

    @SuppressWarnings("unchecked")
    public Streamable apply(long index, Streamable event) {
        ResourceHandler handler = handlers.getOrElse(event.getClass(), null);
        if (handler != null) {
            return handler.apply(index, event);
        } else {
            logger.warn("resource handler not found: {}", event);
            return null;
        }
    }

    public RaftSnapshot prepareSnapshot(RaftSnapshotMetadata snapshotMeta) {
        Vector<Streamable> snapshots = resources.flatMap(r -> r.prepareSnapshot(snapshotMeta));
        return new RaftSnapshot(snapshotMeta, snapshots);
    }

    @FunctionalInterface
    public interface ResourceHandler<T> {
        Streamable apply(long index, T event);
    }
}
