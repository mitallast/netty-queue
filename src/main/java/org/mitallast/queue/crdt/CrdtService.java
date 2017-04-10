package org.mitallast.queue.crdt;

import java.util.Optional;

public interface CrdtService {

    boolean createLWWRegister(long id);

    boolean remove(long id);

    Crdt crdt(long id);

    Optional<Crdt> crdtOpt(long id);

    <T extends Crdt> T crdt(long id, Class<T> type);
}
