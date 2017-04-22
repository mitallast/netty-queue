package org.mitallast.queue.crdt.registry;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import javaslang.control.Option;
import org.mitallast.queue.crdt.Crdt;
import org.mitallast.queue.crdt.commutative.GCounter;
import org.mitallast.queue.crdt.commutative.LWWRegister;
import org.mitallast.queue.crdt.replication.Replicator;

import java.util.concurrent.locks.ReentrantLock;

public class DefaultCrdtRegistry implements CrdtRegistry {

    private final int index;
    private final long replica;
    private final Replicator replicator;
    private final ReentrantLock lock = new ReentrantLock();

    private TLongObjectMap<Crdt> crdtMap = new TLongObjectHashMap<>();

    @Inject
    public DefaultCrdtRegistry(
        @Assisted int index,
        @Assisted long replica,
        @Assisted Replicator replicator
    ) {
        this.index = index;
        this.replica = replica;
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
            crdtMap.put(id, new LWWRegister(id, replicator));
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean createGCounter(long id) {
        lock.lock();
        try {
            if (crdtMap.containsKey(id)) {
                return false;
            }
            crdtMap.put(id, new GCounter(id, replica, replicator));
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
            crdtMap.remove(id);
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
    public Option<Crdt> crdtOpt(long id) {
        return Option.of(crdtMap.get(id));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends Crdt> T crdt(long id, Class<T> type) {
        Crdt crdt = crdt(id);
        if (type.isInstance(crdt)) {
            return (T) crdt;
        } else {
            throw new IllegalArgumentException("CRDT " + id + " does not " + type.getSimpleName() + ", actual " + crdt.getClass().getSimpleName());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends Crdt> Option<T> crdtOpt(long id, Class<T> type) {
        Crdt crdt = crdtMap.get(id);
        if (crdt == null) {
            return Option.none();
        }
        if (type.isInstance(crdt)) {
            return Option.some((T) crdt);
        } else {
            throw new IllegalArgumentException("CRDT " + id + " does not " + type.getSimpleName() + ", actual " + crdt.getClass().getSimpleName());
        }
    }
}
