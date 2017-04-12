package org.mitallast.queue.crdt.log;

import org.mitallast.queue.crdt.Crdt;
import org.mitallast.queue.crdt.registry.CrdtRegistry;

import javax.inject.Inject;
import java.util.Optional;
import java.util.function.Predicate;

public class DefaultCompactionFilter implements Predicate<LogEntry> {
    private final CrdtRegistry crdtRegistry;

    @Inject
    public DefaultCompactionFilter(CrdtRegistry crdtRegistry) {
        this.crdtRegistry = crdtRegistry;
    }

    @Override
    public boolean test(LogEntry logEntry) {
        Optional<Crdt> crdt = crdtRegistry.crdtOpt(logEntry.id());
        return !crdt.isPresent() || crdt.get().shouldCompact(logEntry.event());
    }
}
