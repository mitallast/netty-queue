package org.mitallast.queue.raft.log.compaction;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.mitallast.queue.common.concurrent.Futures;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.log.Segment;
import org.mitallast.queue.log.SegmentDescriptor;
import org.mitallast.queue.log.SegmentDescriptorService;
import org.mitallast.queue.log.SegmentManager;
import org.mitallast.queue.raft.log.SegmentRaftLog;
import org.mitallast.queue.raft.log.entry.EntryFilter;
import org.mitallast.queue.raft.log.entry.RaftLogEntry;
import org.mitallast.queue.raft.util.ExecutionContext;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class MinorCompaction extends Compaction {
    private final EntryFilter filter;
    private final SegmentManager segmentManager;
    private final SegmentDescriptorService descriptorService;
    private final ExecutionContext executionContext;

    @Inject
    public MinorCompaction(
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
        this.descriptorService = raftLog.segmentManager().descriptorService();
    }

    @Override
    public Type type() {
        return Type.MINOR;
    }

    @Override
    CompletableFuture<Void> run() {
        CompletableFuture<Void> future = Futures.future();
        executionContext.execute(() -> {
            logger.info("Compacting the log");
            setRunning(true);
            compactLevels(getCompactSegments().iterator(), future)
                .whenComplete((result, error) -> setRunning(false));
        });
        return future;
    }

    /**
     * Returns a list of segment levels to compact.
     */
    private List<List<Segment>> getCompactSegments() {
        List<List<Segment>> allSegments = new ArrayList<>();
        SortedMap<Long, List<Segment>> levels = createLevels();

        // Given a sorted list of segment levels, iterate through segments to find a level that should be compacted.
        // Compaction eligibility is determined based on the level and compaction factor.
        for (Map.Entry<Long, List<Segment>> entry : levels.entrySet()) {
            long version = entry.getKey();
            List<Segment> level = entry.getValue();
            if (level.stream().mapToLong(Segment::size).sum() > Math.pow(descriptorService.getMaxSegmentSize(), version - 1)) {
                allSegments.add(level);
            }
        }
        return allSegments;
    }

    /**
     * Creates a map of level numbers to segments.
     */
    private SortedMap<Long, List<Segment>> createLevels() {
        // Iterate through segments from oldest to newest and create a map of levels based on segment versions. Because of
        // the nature of this compaction strategy, segments of the same level should always be next to one another.
        TreeMap<Long, List<Segment>> levels = new TreeMap<>();
        for (Segment segment : segmentManager.segments()) {
            if (segment.lastIndex() <= index()) {
                List<Segment> level = levels.get(segment.descriptor().version());
                if (level == null) {
                    level = new ArrayList<>();
                    levels.put(segment.descriptor().version(), level);
                }
                level.add(segment);
            }
        }
        return levels;
    }

    /**
     * Compacts all levels.
     */
    private CompletableFuture<Void> compactLevels(Iterator<List<Segment>> iterator, CompletableFuture<Void> future) {
        if (iterator.hasNext()) {
            compactLevel(iterator.next(), Futures.future()).whenCompleteAsync((result, error) -> {
                if (error == null) {
                    compactLevels(iterator, future);
                } else {
                    future.completeExceptionally(error);
                }
            }, executionContext.executor());
        } else {
            future.complete(null);
        }
        return future;
    }

    /**
     * Compacts a level.
     */
    private CompletableFuture<Void> compactLevel(List<Segment> segments, CompletableFuture<Void> future) {
        logger.info("compacting {}", segments);

        // Copy the list of segments. We'll be removing segments as they're compacted, but we need to remember the full
        // list of segments for finalizing the compaction as well.
        List<Segment> levelSegments = new ArrayList<>(segments);

        // Remove the first segment from the level.
        Segment segment = levelSegments.remove(0);

        // Create an initial compact segment.
        Segment compactSegment;
        try {
            SegmentDescriptor next = segment.descriptor().nextVersion();
            compactSegment = segmentManager.createSegment(next);
        } catch (IOException e) {
            future.completeExceptionally(e);
            return future;
        }

        // Create a list of compact segments. This list will track all segments used to compact the level.
        List<Segment> compactSegments = new ArrayList<>();
        compactSegments.add(compactSegment);

        compactSegments(segment, segment.firstIndex(), compactSegment, levelSegments, compactSegments, Futures.future())
            .whenCompleteAsync((result, error) -> {
                if (error == null) {
                    try {
                        updateSegments(segments, result);
                        future.complete(null);
                    } catch (IOException e) {
                        future.completeExceptionally(e);
                    }
                } else {
                    future.completeExceptionally(error);
                }
            }, executionContext.executor());
        return future;
    }

    /**
     * Compacts a set of segments in the level.
     */
    private CompletableFuture<List<Segment>> compactSegments(
        Segment segment,
        long index,
        Segment compactSegment,
        List<Segment> segments,
        List<Segment> compactSegments,
        CompletableFuture<List<Segment>> future
    ) {
        // Read the entry from the segment. If the entry is null or filtered out of the log, skip the entry, otherwise
        // append it to the compact segment.
        try {
            RaftLogEntry entry = segment.getEntry(index);
            if (entry != null) {
                if (filter.accept(entry, this)) {
                    compactSegment.appendEntry(entry);
                } else {
                    logger.info("Filtered {} from segment {}", entry, segment.descriptor().id());
                    compactSegment.skip(1);
                }

                if (index == segment.lastIndex()) {
                    if (segments.isEmpty()) {
                        future.complete(compactSegments);
                    } else {
                        Segment nextSegment = segments.remove(0);
                        Segment nextCompactSegment = nextCompactSegment(nextSegment, compactSegment, compactSegments);
                        compactSegments(nextSegment, nextSegment.firstIndex(), nextCompactSegment, segments, compactSegments, future);
                    }
                } else {
                    Segment nextCompactSegment = nextCompactSegment(segment, compactSegment, compactSegments);
                    compactSegments(segment, index + 1, nextCompactSegment, segments, compactSegments, future);
                }
            } else {
                compactSegment.skip(1);
            }
        } catch (IOException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    /**
     * Returns the next compact segment for the given segment and compact segment.
     */
    private Segment nextCompactSegment(Segment segment, Segment compactSegment, List<Segment> compactSegments) throws IOException {
        if (compactSegment.isFull()) {
            SegmentDescriptor descriptor = segment.descriptor().nextVersion();
            Segment newSegment = segmentManager.createSegment(descriptor);
            compactSegments.add(newSegment);
            return newSegment;
        } else {
            return compactSegment;
        }
    }

    /**
     * Updates the log segments.
     */
    private void updateSegments(List<Segment> segments, List<Segment> compactSegments) throws IOException {
        Set<Long> segmentIds = segments.stream().map(s -> s.descriptor().id()).collect(Collectors.toSet());
        Map<Long, Segment> mappedSegments = compactSegments.stream().collect(Collectors.toMap(s -> s.descriptor().id(), s -> s));

        List<Segment> updatedSegments = new ArrayList<>();
        for (Segment segment : segmentManager.segments()) {
            if (!segmentIds.contains(segment.descriptor().id())) {
                updatedSegments.add(segment);
            } else {
                Segment compactSegment = mappedSegments.get(segment.descriptor().id());
                if (compactSegment != null) {
                    updatedSegments.add(compactSegment);
                }
            }
        }

        segmentManager.update(updatedSegments);
    }

}
