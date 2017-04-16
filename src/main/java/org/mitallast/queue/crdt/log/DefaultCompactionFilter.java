package org.mitallast.queue.crdt.log;

import org.mitallast.queue.crdt.registry.CrdtRegistry;

import javax.inject.Inject;
import java.util.function.Predicate;

public class DefaultCompactionFilter implements Predicate<LogEntry> {
    private final CrdtRegistry crdtRegistry;

    @Inject
    public DefaultCompactionFilter(CrdtRegistry crdtRegistry) {
        this.crdtRegistry = crdtRegistry;
    }

    @Override
    public boolean test(LogEntry logEntry) {
        return crdtRegistry.crdtOpt(logEntry.id())
            .map(crdt -> crdt.shouldCompact(logEntry.event()))
            .getOrElse(true);
    }
}
