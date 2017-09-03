package org.mitallast.queue.crdt.log

import org.mitallast.queue.crdt.registry.CrdtRegistry
import java.util.function.Predicate
import javax.inject.Inject

class DefaultCompactionFilter @Inject constructor(private val crdtRegistry: CrdtRegistry) : Predicate<LogEntry> {

    override fun test(logEntry: LogEntry): Boolean {
        return crdtRegistry.crdtOpt(logEntry.id)
            .map { crdt -> crdt.shouldCompact(logEntry.event) }
            .getOrElse(true)
    }
}
