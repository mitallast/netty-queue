package org.mitallast.queue.raft.resource;

import javaslang.collection.HashMap;
import javaslang.collection.Map;
import javaslang.collection.Vector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.raft.protocol.RaftSnapshot;
import org.mitallast.queue.raft.protocol.RaftSnapshotMetadata;

public class ResourceRegistry {
    private final static Logger logger = LogManager.getLogger();

    private volatile Vector<ResourceFSM> resources = Vector.empty();
    private volatile Map<Class, ResourceHandler> handlers = HashMap.empty();

    public synchronized void register(ResourceFSM fsm) {
        resources = resources.append(fsm);
    }

    public synchronized <T extends Message> void register(Class<T> type, ResourceHandler<T> handler) {
        handlers = handlers.put(type, handler);
    }

    @SuppressWarnings("unchecked")
    public Message apply(long index, Message event) {
        ResourceHandler handler = handlers.getOrElse(event.getClass(), null);
        if (handler != null) {
            return handler.apply(index, event);
        } else {
            logger.warn("resource handler not found: {}", event);
            return null;
        }
    }

    public RaftSnapshot prepareSnapshot(RaftSnapshotMetadata snapshotMeta) {
        Vector<Message> snapshots = resources.flatMap(r -> r.prepareSnapshot(snapshotMeta));
        return new RaftSnapshot(snapshotMeta, snapshots);
    }

    @FunctionalInterface
    public interface ResourceHandler<T extends Message> {
        Message apply(long index, T event);
    }
}
