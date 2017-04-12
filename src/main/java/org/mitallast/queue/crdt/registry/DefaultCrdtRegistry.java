package org.mitallast.queue.crdt.registry;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.mitallast.queue.common.collection.ImmutableLongMap;
import org.mitallast.queue.crdt.Crdt;
import org.mitallast.queue.crdt.commutative.LWWRegister;
import org.mitallast.queue.crdt.replication.Replicator;

import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

public class DefaultCrdtRegistry implements CrdtRegistry {

    private final int index;
    private final Replicator replicator;
    private final ReentrantLock lock = new ReentrantLock();

    private volatile ImmutableLongMap<Crdt> crdtMap = ImmutableLongMap.empty();

    @Inject
    public DefaultCrdtRegistry(
        @Assisted int index,
        @Assisted Replicator replicator
    ) {
        this.index = index;
        this.replicator = replicator;
    }

    @Override
    public int index() {
        return index;
    }

    @Override
    public boolean createLWWRegister(long id) {
        lock.lock();
        try {
            if (crdtMap.containsKey(id)) {
                return false;
            }
            LWWRegister crdt = new LWWRegister(id, replicator);
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

    @Override
    @SuppressWarnings("unchecked")
    public <T extends Crdt> Optional<T> crdtOpt(long id, Class<T> type) {
        Crdt crdt = crdtMap.get(id);
        if (crdt == null) {
            return Optional.empty();
        }
        if (type.isInstance(crdt)) {
            return Optional.of((T) crdt);
        } else {
            throw new IllegalArgumentException("CRDT " + id + " does not LWWRegister, actual " + crdt.getClass().getSimpleName());
        }
    }
}
