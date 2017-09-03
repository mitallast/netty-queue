package org.mitallast.queue.crdt.registry

import com.google.inject.Inject
import com.google.inject.assistedinject.Assisted
import gnu.trove.map.hash.TLongObjectHashMap
import javaslang.control.Option
import org.mitallast.queue.crdt.Crdt
import org.mitallast.queue.crdt.commutative.GCounter
import org.mitallast.queue.crdt.commutative.GSet
import org.mitallast.queue.crdt.commutative.LWWRegister
import org.mitallast.queue.crdt.commutative.OrderedGSet
import org.mitallast.queue.crdt.replication.Replicator
import java.util.concurrent.locks.ReentrantLock

@Suppress("UNCHECKED_CAST")
class DefaultCrdtRegistry @Inject constructor(
    @param:Assisted private val index: Int,
    @param:Assisted private val replica: Long,
    @param:Assisted private val replicator: Replicator
) : CrdtRegistry {
    private val lock = ReentrantLock()
    private val crdtMap = TLongObjectHashMap<Crdt>()

    override fun index(): Int = index

    override fun createLWWRegister(id: Long): Boolean {
        lock.lock()
        try {
            if (crdtMap.containsKey(id)) {
                return false
            }
            crdtMap.put(id, LWWRegister(id, replicator))
            return true
        } finally {
            lock.unlock()
        }
    }

    override fun createGCounter(id: Long): Boolean {
        lock.lock()
        try {
            if (crdtMap.containsKey(id)) {
                return false
            }
            crdtMap.put(id, GCounter(id, replica, replicator))
            return true
        } finally {
            lock.unlock()
        }
    }

    override fun createGSet(id: Long): Boolean {
        lock.lock()
        try {
            if (crdtMap.containsKey(id)) {
                return false
            }
            crdtMap.put(id, GSet(id, replicator))
            return true
        } finally {
            lock.unlock()
        }
    }

    override fun createOrderedGSet(id: Long): Boolean {
        lock.lock()
        try {
            if (crdtMap.containsKey(id)) {
                return false
            }
            crdtMap.put(id, OrderedGSet(id, replica, replicator))
            return true
        } finally {
            lock.unlock()
        }
    }

    override fun remove(id: Long): Boolean {
        lock.lock()
        try {
            if (crdtMap.containsKey(id)) {
                return false
            }
            crdtMap.remove(id)
            return true
        } finally {
            lock.unlock()
        }
    }

    override fun crdt(id: Long): Crdt {
        return crdtMap.get(id) ?: throw IllegalArgumentException("CRDT $id not registered")
    }

    override fun crdtOpt(id: Long): Option<Crdt> {
        return Option.of(crdtMap.get(id))
    }

    override fun <T : Crdt> crdt(id: Long, type: Class<T>): T {
        val crdt = crdt(id)
        return if (type.isInstance(crdt)) {
            crdt as T
        } else {
            throw IllegalArgumentException("CRDT " + id + " does not " + type.simpleName + ", actual " + crdt.javaClass.simpleName)
        }
    }

    override fun <T : Crdt> crdtOpt(id: Long, type: Class<T>): Option<T> {
        val crdt = crdtMap.get(id) ?: return Option.none()
        return if (type.isInstance(crdt)) {
            Option.some(crdt as T)
        } else {
            throw IllegalArgumentException("CRDT " + id + " does not " + type.simpleName + ", actual " + crdt.javaClass.simpleName)
        }
    }
}
