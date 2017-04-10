package org.mitallast.queue.crdt;

import com.google.inject.Inject;
import org.mitallast.queue.common.collection.ImmutableLongMap;
import org.mitallast.queue.crdt.commutative.LWWRegister;
import org.mitallast.queue.crdt.protocol.Append;
import org.mitallast.queue.crdt.replication.Replicator;

import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

public class DefaultCrdtService implements CrdtService {

    private final Replicator replicator;
    private final ReentrantLock lock = new ReentrantLock();

    private volatile ImmutableLongMap<Crdt> crdtMap = ImmutableLongMap.empty();

    @Inject
    public DefaultCrdtService(Replicator replicator) {
        this.replicator = replicator;
    }

    @Override
    public boolean createLWWRegister(long id) {
        lock.lock();
        try {
            if (crdtMap.containsKey(id)) {
                return false;
            }
            LWWRegister crdt = new LWWRegister(event -> replicator.handle(new Append(id, event)));
            crdtMap = ImmutableLongMap.<Crdt>builder()
                .putAll(crdtMap)
                .put(id, crdt)
                .build();
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean remove(long id) {
        lock.lock();
        try {
            if (crdtMap.containsKey(id)) {
                return false;
            }
            crdtMap = crdtMap.remove(id);
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Crdt crdt(long id) {
        Crdt crdt = crdtMap.get(id);
        if (crdt == null) {
            throw new IllegalArgumentException("CRDT " + id + " not registered");
        }
        return crdt;
    }

    @Override
    public Optional<Crdt> crdtOpt(long id) {
        return Optional.ofNullable(crdtMap.get(id));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends Crdt> T crdt(long id, Class<T> type) {
        Crdt crdt = crdt(id);
        if (type.isInstance(crdt)) {
            return (T) crdt;
        } else {
            throw new IllegalArgumentException("CRDT " + id + " does not LWWRegister, actual " + crdt.getClass().getSimpleName());
        }
    }
}
