package org.mitallast.queue.crdt.registry

import io.vavr.control.Option
import org.mitallast.queue.crdt.Crdt

interface CrdtRegistry {

    fun index(): Int

    fun createLWWRegister(id: Long): Boolean

    fun createGCounter(id: Long): Boolean

    fun createGSet(id: Long): Boolean

    fun createOrderedGSet(id: Long): Boolean

    fun remove(id: Long): Boolean

    fun crdt(id: Long): Crdt

    fun crdtOpt(id: Long): Option<Crdt>

    fun <T : Crdt> crdt(id: Long, type: Class<T>): T

    fun <T : Crdt> crdtOpt(id: Long, type: Class<T>): Option<T>
}
