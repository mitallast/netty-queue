package org.mitallast.queue.crdt;

import com.google.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.common.collection.ImmutableLongMap;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.crdt.commutative.LWWRegister;
import org.mitallast.queue.crdt.protocol.Append;
import org.mitallast.queue.crdt.replication.Replicator;

import java.util.concurrent.locks.ReentrantLock;

public class CrdtService {
    private final static Logger logger = LogManager.getLogger();

    private final Replicator replicator;
    private final ReentrantLock lock = new ReentrantLock();

    private volatile ImmutableLongMap<Crdt> crdtMap = ImmutableLongMap.empty();

    @Inject
    public CrdtService(Replicator replicator) {
        this.replicator = replicator;
    }

    public void createLWWRegister(long id) {
        lock.lock();
        try {
            crdtMap = ImmutableLongMap.<Crdt>builder()
                .putAll(crdtMap)
                .put(id, new LWWRegister(event -> replicator.handle(new Append(id, event))))
                .build();
        } finally {
            lock.unlock();
        }
    }

    public void update(long id, Streamable event) {
        Crdt crdt = crdtMap.get(id);
        if (crdt == null) {
            logger.warn("crdt {} does not exist", id);
        } else {
            crdt.update(event);
        }
    }
}
