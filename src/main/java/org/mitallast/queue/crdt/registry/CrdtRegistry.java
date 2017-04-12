package org.mitallast.queue.crdt.registry;

import org.mitallast.queue.crdt.Crdt;

import java.util.Optional;

public interface CrdtRegistry {

    int index();

    boolean createLWWRegister(long id);

    boolean remove(long id);

    Crdt crdt(long id);

    Optional<Crdt> crdtOpt(long id);

    <T extends Crdt> T crdt(long id, Class<T> type);

    <T extends Crdt> Optional<T> crdtOpt(long id, Class<T> type);
}
