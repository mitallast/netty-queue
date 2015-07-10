package org.mitallast.queue.raft.log;

import org.mitallast.queue.common.concurrent.Futures;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.raft.log.entry.EntryFilter;
import org.mitallast.queue.raft.log.entry.LogEntry;
import org.mitallast.queue.raft.util.ExecutionContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class MajorCompaction extends Compaction {
    private final EntryFilter filter;
    private final ExecutionContext executionContext;

    public MajorCompaction(Settings settings, long index, EntryFilter filter, ExecutionContext executionContext) {
        super(settings, index);
        this.filter = filter;
        this.executionContext = executionContext;
    }

    @Override
    public Type type() {
        return Type.MAJOR;
    }

    @Override
    CompletableFuture<Void> run(SegmentManager segments) {
        CompletableFuture<Void> future = Futures.future();
        executionContext.execute(() -> {
            logger.info("Compacting the log");
            setRunning(true);
            compactSegments(getActiveSegments(segments).iterator(), segments, future)
                .whenComplete((result, error) -> setRunning(false));
        });
        return future;
    }

    private List<Segment> getActiveSegments(SegmentManager manager) {
        List<Segment> segments = manager.segments().stream()
            .filter(segment -> !segment.isEmpty() && segment.lastIndex() <= index())
            .collect(Collectors.toList());
        logger.info("found {} compactable segments", segments.size());
        return segments;
    }

    private CompletableFuture<Void> compactSegments(Iterator<Segment> iterator, SegmentManager manager, CompletableFuture<Void> future) {
        if (iterator.hasNext()) {
            compactSegment(iterator.next(), manager).whenCompleteAsync((result, error) -> {
                executionContext.checkThread();
                if (error == null) {
                    compactSegments(iterator, manager, future);
                } else {
                    future.completeExceptionally(error);
                }
            }, executionContext.executor());
        } else {
            future.complete(null);
        }
        return future;
    }

    private CompletableFuture<Void> compactSegment(Segment segment, SegmentManager manager) {
        return shouldCompactSegment(segment, Futures.future()).thenCompose(compact -> {
            if (compact) {
                logger.info("compacting {}", segment);
                Segment compactSegment;
                try {
                    SegmentDescriptor next = segment.descriptor().nextVersion();
                    compactSegment = manager.createSegment(next);
                } catch (IOException e) {
                    return Futures.completeExceptionally(e);
                }
                return compactSegment(segment, segment.firstIndex(), compactSegment, Futures.future())
                    .thenAcceptAsync((replacedSegment) -> {
                        try {
                            manager.replace(replacedSegment);
                        } catch (IOException e) {
                            logger.error("error replace", e);
                        }
                    }, executionContext.executor());
            } else {
                return org.mitallast.queue.common.concurrent.Futures.complete(null);
            }
        });
    }

    private CompletableFuture<Segment> compactSegment(Segment segment, long index, Segment compactSegment, CompletableFuture<Segment> future) {
        try {
            LogEntry entry = segment.getEntry(index);
            if (entry != null) {
                if (filter.accept(entry, this)) {
                    compactSegment.appendEntry(entry);
                } else {
                    logger.info("Filtered {} from segment {}", entry, segment.descriptor().id());
                    compactSegment.skip(1);
                }

                if (index == segment.lastIndex()) {
                    future.complete(compactSegment);
                } else {
                    compactSegment(segment, index + 1, compactSegment, future);
                }
            } else {
                compactSegment.skip(1);

                if (index == segment.lastIndex()) {
                    future.complete(compactSegment);
                } else {
                    executionContext.execute(() -> compactSegment(segment, index + 1, compactSegment, future));
                }
            }
            return future;
        } catch (IOException e) {
            return Futures.completeExceptionally(e);
        }
    }

    private CompletableFuture<Boolean> shouldCompactSegment(Segment segment, CompletableFuture<Boolean> future) {
        logger.info("evaluating {} for major compaction", segment);
        return shouldCompactSegment(segment, segment.firstIndex(), future);
    }

    private CompletableFuture<Boolean> shouldCompactSegment(Segment segment, long index, CompletableFuture<Boolean> future) {
        try {
            LogEntry entry = segment.getEntry(index);
            if (entry != null) {
                if (!filter.accept(entry, this)) {
                    future.complete(true);
                } else if (index == segment.lastIndex()) {
                    future.complete(false);
                } else {
                    shouldCompactSegment(segment, index + 1, future);
                }
            } else if (index == segment.lastIndex()) {
                future.complete(false);
            } else {
                executionContext.execute(() -> shouldCompactSegment(segment, index + 1, future));
            }
            return future;
        } catch (IOException e) {
            return Futures.completeExceptionally(e);
        }
    }

}
