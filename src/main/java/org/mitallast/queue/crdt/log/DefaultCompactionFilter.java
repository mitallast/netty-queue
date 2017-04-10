package org.mitallast.queue.crdt.log;

import org.mitallast.queue.crdt.Crdt;
import org.mitallast.queue.crdt.CrdtService;

import javax.inject.Inject;
import java.util.Optional;
import java.util.function.Predicate;

public class DefaultCompactionFilter implements Predicate<LogEntry> {
    private final CrdtService crdtService;

    @Inject
    public DefaultCompactionFilter(CrdtService crdtService) {
        this.crdtService = crdtService;
    }

    @Override
    public boolean test(LogEntry logEntry) {
        Optional<Crdt> crdt = crdtService.crdtOpt(logEntry.id());
        return !crdt.isPresent() || crdt.get().shouldCompact(logEntry.event());
    }
}
