package org.mitallast.queue.log;

import com.google.common.collect.ImmutableSortedMap;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.settings.Settings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class SegmentManager extends AbstractLifecycleComponent {
    private final SegmentDescriptorService descriptorService;
    private final SegmentService segmentService;

    private Segment currentSegment;
    private ImmutableSortedMap<Long, Segment> segments = ImmutableSortedMap.of();

    public SegmentManager(Settings settings, SegmentDescriptorService descriptorService, SegmentService segmentService) {
        super(settings);
        this.descriptorService = descriptorService;
        this.segmentService = segmentService;
    }

    public SegmentDescriptorService descriptorService() {
        return descriptorService;
    }

    @Override
    protected void doStart() throws IOException {
        // Load existing log segments from disk.
        ImmutableSortedMap.Builder<Long, Segment> builder = ImmutableSortedMap.naturalOrder();
        for (Segment segment : loadSegments()) {
            builder.put(segment.descriptor().index(), segment);
        }
        segments = builder.build();

        if (!segments.isEmpty()) {
            currentSegment = segments.lastEntry().getValue();
        } else {
            SegmentDescriptor descriptor = descriptorService.createDescriptor(1, 1, 1);
            currentSegment = segmentService.createSegment(descriptor);
            segments = ImmutableSortedMap.of(1l, currentSegment);
        }
    }

    @Override
    protected void doStop() throws IOException {
        for (Segment segment : segments.values()) {
            logger.info("closing segment: {}", segment.descriptor().id());
            try {
                segment.close();
            } catch (IOException e) {
                logger.error("error close segment", e);
            }
        }
        segments = ImmutableSortedMap.of();
        currentSegment = null;
    }

    @Override
    protected void doClose() throws IOException {
    }

    public void delete() throws IOException {
        for (Segment segment : loadSegments()) {
            deleteSegment(segment);
        }
    }

    public Segment firstSegment() {
        Map.Entry<Long, Segment> segment = segments.firstEntry();
        return segment != null ? segment.getValue() : null;
    }

    public Segment lastSegment() {
        Map.Entry<Long, Segment> segment = segments.lastEntry();
        return segment != null ? segment.getValue() : null;
    }

    public Segment currentSegment() {
        return currentSegment != null ? currentSegment : lastSegment();
    }

    public Segment nextSegment() throws IOException {
        Segment lastSegment = lastSegment();
        SegmentDescriptor descriptor = descriptorService.createDescriptor(
            lastSegment != null ? lastSegment.descriptor().id() + 1 : 1,
            currentSegment.lastIndex() + 1,
            1
        );
        currentSegment = createSegment(descriptor);
        segments = ImmutableSortedMap.<Long, Segment>naturalOrder()
            .putAll(segments)
            .put(descriptor.index(), currentSegment)
            .build();
        return currentSegment;
    }

    public Collection<Segment> segments() {
        return segments.values();
    }

    public Segment segment(long index) {
        // Check if the current segment contains the given index first in order to prevent an unnecessary map lookup.
        if (currentSegment != null && currentSegment.containsIndex(index))
            return currentSegment;

        // If the index is in another segment, get the entry with the next lowest first index.
        Map.Entry<Long, Segment> segment = segments.floorEntry(index);
        return segment != null ? segment.getValue() : null;
    }

    public void remove(Segment removed) throws IOException {
        currentSegment = null;

        ImmutableSortedMap<Long, Segment> removalSegments = segments.tailMap(removed.descriptor().index());

        ImmutableSortedMap.Builder<Long, Segment> builder = ImmutableSortedMap.naturalOrder();
        segments.entrySet().stream()
            .filter(e -> !removalSegments.containsKey(e.getKey()))
            .forEach(builder::put);
        segments = builder.build();

        for (Segment segment : removalSegments.values()) {
            deleteSegment(segment);
        }
        resetCurrentSegment();
    }

    private void resetCurrentSegment() throws IOException {
        Segment lastSegment = lastSegment();
        if (lastSegment != null) {
            currentSegment = lastSegment;
        } else {
            SegmentDescriptor descriptor = descriptorService.createDescriptor(1, 1, 1);
            currentSegment = createSegment(descriptor);
            segments = ImmutableSortedMap.<Long, Segment>naturalOrder()
                .putAll(segments)
                .put(1L, currentSegment)
                .build();
        }
    }

    public Segment createSegment(SegmentDescriptor descriptor) throws IOException {
        Segment segment = segmentService.createSegment(descriptor);
        logger.info("created segment: {}", segment);
        return segment;
    }

    private Collection<Segment> loadSegments() throws IOException {
        // Once we've constructed a map of the most recent descriptors, load the segments.
        List<Segment> segments = new ArrayList<>();
        for (SegmentDescriptor descriptor : descriptorService.loadDescriptors()) {
            segments.add(loadSegment(descriptor));
        }
        return segments;
    }

    private Segment loadSegment(SegmentDescriptor descriptor) throws IOException {
        Segment segment = segmentService.createSegment(descriptor);
        logger.info("loaded segment: {}:{}", descriptor.id(), descriptor.version());
        return segment;
    }

    public void replace(Segment segment) throws IOException {
        Segment oldSegment = segments.get(segment.descriptor().index());

        ImmutableSortedMap.Builder<Long, Segment> builder = ImmutableSortedMap.naturalOrder();
        segments.entrySet().stream()
            .filter(e -> e.getKey() != segment.descriptor().index())
            .forEach(builder::put);
        builder.put(segment.descriptor().index(), segment);
        segments = builder.build();

        if (oldSegment != null) {
            deleteSegment(oldSegment);
        }
    }

    public void update(Collection<Segment> segments) throws IOException {
        ImmutableSortedMap.Builder<Long, Segment> builder = ImmutableSortedMap.naturalOrder();
        segments.forEach(s -> builder.put(s.descriptor().index(), s));
        ImmutableSortedMap<Long, Segment> newSegments = builder.build();

        // Assign the new segments map and delete any segments that were removed from the map.
        ImmutableSortedMap<Long, Segment> oldSegments = this.segments;
        this.segments = newSegments;
        resetCurrentSegment();

        // Deletable segments are determined by whether the segment does not have a matching segment/version in the new segments.
        for (Segment oldSegment : oldSegments.values()) {
            Segment segment = this.segments.get(oldSegment.descriptor().index());
            if (segment == null || segment.descriptor().id() != oldSegment.descriptor().id() || segment.descriptor().version() > oldSegment.descriptor().version()) {
                deleteSegment(segment);
            }
        }
    }

    private void deleteSegment(Segment segment) throws IOException {
        logger.info("deleting segment: {}:{}", segment.descriptor().id(), segment.descriptor().version());
        segment.close();
        descriptorService.deleteDescriptor(segment.descriptor());
        segmentService.deleteSegment(segment.descriptor());
    }
}
