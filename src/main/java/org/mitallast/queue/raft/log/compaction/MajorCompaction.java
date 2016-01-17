package org.mitallast.queue.raft.log.compaction;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.mitallast.queue.common.concurrent.Futures;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.log.Segment;
import org.mitallast.queue.log.SegmentDescriptor;
import org.mitallast.queue.log.SegmentManager;
import org.mitallast.queue.raft.log.SegmentRaftLog;
import org.mitallast.queue.raft.log.entry.EntryFilter;
import org.mitallast.queue.raft.log.entry.RaftLogEntry;
import org.mitallast.queue.raft.util.ExecutionContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class MajorCompaction extends Compaction {
    private final EntryFilter filter;
    private final SegmentManager segmentManager;
    private final ExecutionContext executionContext;

    @Inject
    public MajorCompaction(
        Settings settings,
        ExecutionContext executionContext,
        EntryFilter filter,
        SegmentRaftLog raftLog,
        @Assisted long index
    ) {
        super(settings, index);
        this.filter = filter;
        this.executionContext = executionContext;
        this.segmentManager = raftLog.segmentManager();
    }

    @Override
    public Type type() {
        return Type.MAJOR;
    }

    @Override
    CompletableFuture<Void> run() {
        CompletableFuture<Void> future = Futures.future();
        executionContext.execute("compact log", () -> {
            logger.info("Compacting the log");
            setRunning(true);
            compactSegments(getActiveSegments().iterator(), future)
                .whenComplete((result, error) -> setRunning(false));
        });
        return future;
    }

    private List<Segment> getActiveSegments() {
        List<Segment> segments = segmentManager.segments().stream()
            .filter(segment -> !segment.isEmpty() && segment.lastIndex() <= index())
            .collect(Collectors.toList());
        logger.info("found {} compactable segments", segments.size());
        return segments;
    }

    private CompletableFuture<Void> compactSegments(Iterator<Segment> iterator, CompletableFuture<Void> future) {
        if (iterator.hasNext()) {
            compactSegment(iterator.next()).whenCompleteAsync((result, error) -> {
                executionContext.checkThread();
                if (error == null) {
                    compactSegments(iterator, future);
                } else {
                    future.completeExceptionally(error);
                }
            }, executionContext.executor("compact segment"));
        } else {
            future.complete(null);
        }
        return future;
    }

    private CompletableFuture<Void> compactSegment(Segment segment) {
        return shouldCompactSegment(segment, Futures.future()).thenCompose(compact -> {
            if (compact) {
                logger.info("compacting {}", segment);
                Segment compactSegment;
                try {
                    SegmentDescriptor next = segment.descriptor().nextVersion();
                    compactSegment = segmentManager.createSegment(next);
                } catch (IOException e) {
                    return Futures.completeExceptionally(e);
                }
                return compactSegment(segment, segment.firstIndex(), compactSegment, Futures.future())
                    .thenAcceptAsync((replacedSegment) -> {
                        try {
                            segmentManager.replace(replacedSegment);
                        } catch (IOException e) {
                            logger.error("error replace", e);
                        }
                    }, executionContext.executor("replace segment"));
            } else {
                return org.mitallast.queue.common.concurrent.Futures.complete(null);
            }
        });
    }

    private CompletableFuture<Segment> compactSegment(Segment segment, long index, Segment compactSegment, CompletableFuture<Segment> future) {
        try {
            RaftLogEntry entry = segment.getEntry(index);
            if (entry != null) {
                if (filter.accept(entry, this)) {
                    compactSegment.appendEntry(entry);
                } else {
                    logger.info("Filtered {} from segment {}", entry, segment.descriptor().id());
                    long skip = compactSegment.skip(1);
                    assert skip == 1;
                }

                if (index == segment.lastIndex()) {
                    future.complete(compactSegment);
                } else {
                    compactSegment(segment, index + 1, compactSegment, future);
                }
            } else {
                long skip = compactSegment.skip(1);
                assert skip == 1;

                if (index == segment.lastIndex()) {
                    future.complete(compactSegment);
                } else {
                    executionContext.execute("compact segment", () -> compactSegment(segment, index + 1, compactSegment, future));
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
            RaftLogEntry entry = segment.getEntry(index);
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
                executionContext.execute("should compact segment", () -> shouldCompactSegment(segment, index + 1, future));
            }
            return future;
        } catch (IOException e) {
            return Futures.completeExceptionally(e);
        }
    }

}
