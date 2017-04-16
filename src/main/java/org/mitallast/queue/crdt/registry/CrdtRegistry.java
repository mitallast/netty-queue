package org.mitallast.queue.crdt.registry;

import javaslang.control.Option;
import org.mitallast.queue.crdt.Crdt;

public interface CrdtRegistry {

    int index();

    boolean createLWWRegister(long id);

    boolean remove(long id);

    Crdt crdt(long id);

    Option<Crdt> crdtOpt(long id);

    <T extends Crdt> T crdt(long id, Class<T> type);

    <T extends Crdt> Option<T> crdtOpt(long id, Class<T> type);
}
