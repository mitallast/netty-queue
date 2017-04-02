package org.mitallast.queue.crdt;

import org.mitallast.queue.crdt.commutative.LWWRegister;

public interface CrdtService {

    LWWRegister createLWWRegister(long id);

    Crdt crdt(long id);

    <T extends Crdt> T crdt(long id, Class<T> type);
}
