package org.mitallast.queue.raft.log;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.inject.Inject;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.common.unit.ByteSizeUnit;
import org.mitallast.queue.common.unit.ByteSizeValue;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class SegmentManager extends AbstractLifecycleComponent {

    protected final long maxEntrySize;
    protected final long maxSegmentSize;
    protected final long maxEntries;

    private final StreamService streamService;
    private final SegmentFileService fileService;

    private Segment currentSegment;
    private ImmutableSortedMap<Long, Segment> segments = ImmutableSortedMap.of();

    @Inject
    public SegmentManager(Settings settings, StreamService streamService, SegmentFileService fileService) {
        super(settings);
        this.streamService = streamService;
        this.fileService = fileService;

        maxEntrySize = componentSettings.getAsBytesSize("max_entry_size", new ByteSizeValue(100, ByteSizeUnit.MB)).bytes();
        maxSegmentSize = componentSettings.getAsBytesSize("max_segment_size", new ByteSizeValue(100, ByteSizeUnit.MB)).bytes();
        maxEntries = componentSettings.getAsLong("max_entries_per_segment", 1000000l);
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
            SegmentDescriptor descriptor = createDescriptor(1, 1, 1);
            currentSegment = createSegment(descriptor);
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
            logger.info("deleting segment: {}", segment.descriptor().id());
            segment.delete();
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
        SegmentDescriptor descriptor = createDescriptor(
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
            segment.delete();
        }
        resetCurrentSegment();
    }

    private void resetCurrentSegment() throws IOException {
        Segment lastSegment = lastSegment();
        if (lastSegment != null) {
            currentSegment = lastSegment;
        } else {
            SegmentDescriptor descriptor = createDescriptor(1, 1, 1);
            currentSegment = createSegment(descriptor);
            segments = ImmutableSortedMap.<Long, Segment>naturalOrder()
                .putAll(segments)
                .put(1L, currentSegment)
                .build();
        }
    }

    public Segment createSegment(SegmentDescriptor descriptor) throws IOException {
        File segmentFile = fileService.createSegmentFile(descriptor);
        Segment segment = new Segment(segmentFile, descriptor, createIndex(descriptor), streamService);
        logger.info("created segment: {}", segment);
        return segment;
    }

    private Collection<Segment> loadSegments() throws IOException {
        // Once we've constructed a map of the most recent descriptors, load the segments.
        List<Segment> segments = new ArrayList<>();
        for (SegmentDescriptor descriptor : loadDescriptors()) {
            segments.add(loadSegment(descriptor));
        }
        return segments;
    }

    private Segment loadSegment(SegmentDescriptor descriptor) throws IOException {
        File segmentFile = fileService.createSegmentFile(descriptor);
        Segment segment = new Segment(segmentFile, descriptor, createIndex(descriptor), streamService);
        logger.info("loaded segment: {}:{}", descriptor.id(), descriptor.version());
        return segment;
    }

    private SegmentIndex createIndex(SegmentDescriptor descriptor) throws IOException {
        File file = fileService.createIndexFile(descriptor);
        if (file.exists()) {
            if (!file.createNewFile()) {
                throw new IOException("Error create io file");
            }
        }
        return new SegmentIndex(file, (int) descriptor.maxEntries());
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
            logger.info("deleting segment: {}:{}", oldSegment.descriptor().id(), oldSegment.descriptor().version());
            oldSegment.close();
            oldSegment.delete();
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
                logger.info("deleting segment: {}:{}", oldSegment.descriptor().id(), oldSegment.descriptor().version());
                deleteDescriptor(oldSegment.descriptor());
                oldSegment.close();
                oldSegment.delete();
            }
        }
    }

    private SegmentDescriptor createDescriptor(long id, long index, long version) throws IOException {
        SegmentDescriptor descriptor = SegmentDescriptor.builder()
            .setId(id)
            .setIndex(index)
            .setVersion(version)
            .setMaxEntrySize(maxEntrySize)
            .setMaxSegmentSize(maxSegmentSize)
            .setMaxEntries(maxEntries)
            .build();
        File file = fileService.createDescriptorFile(descriptor);
        logger.info("write segment {}:{}", descriptor.id(), descriptor.version());
        try (StreamOutput output = streamService.output(file)) {
            output.writeStreamable(descriptor.toBuilder());
        }
        return descriptor;
    }

    private SegmentDescriptor readDescriptor(File file) throws IOException {
        try (StreamInput input = streamService.input(file)) {
            SegmentFile segmentFile = new SegmentFile(file);
            logger.info("read segment {}:{}", segmentFile.id(), segmentFile.version());
            SegmentDescriptor descriptor = input.readStreamable(SegmentDescriptor.Builder::new).build();
            // Check that the descriptor matches the segment file metadata.
            if (descriptor.id() != segmentFile.id()) {
                throw new DescriptorException(String.format("descriptor ID does not match filename ID: %s", segmentFile.file().getName()));
            }
            if (descriptor.version() != segmentFile.version()) {
                throw new DescriptorException(String.format("descriptor version does not match filename version: %s", segmentFile.file().getName()));
            }
            return descriptor;
        }
    }

    private void deleteDescriptor(SegmentDescriptor descriptor) throws IOException {
        logger.info("delete segment {}:{}", descriptor.id(), descriptor.version());
        File file = fileService.createDescriptorFile(descriptor);
        if (file.exists()) {
            assert file.delete();
        }
    }

    private ImmutableList<SegmentDescriptor> loadDescriptors() throws IOException {
        // Create a map of descriptors for each existing segment in the log. This is done by iterating through the log
        // directory and finding segment files for this log name. For each segment file, check the consistency of the file
        // by comparing versions and locked state in order to prevent lost data from failures during log compaction.
        Map<Long, SegmentDescriptor> descriptors = new HashMap<>();
        for (File file : fileService.listFiles()) {
            if (fileService.isDescriptorFile(file)) {
                SegmentDescriptor descriptor = readDescriptor(file);
                // If a descriptor already exists for the segment, compare the descriptor versions.
                SegmentDescriptor existingDescriptor = descriptors.get(descriptor.id());

                // If this segment's version is greater than the existing segment's version and the segment is locked then
                // overwrite it. The segment will be locked if all entries have been committed, e.g. after compaction.
                if (existingDescriptor == null) {
                    logger.info("found segment: {}:{})", descriptor.id(), descriptor.version());
                    descriptors.put(descriptor.id(), descriptor);
                } else if (descriptor.version() > existingDescriptor.version()) {
                    logger.info("replaced {}:{} with newer version: {}:{}",
                        existingDescriptor.id(), existingDescriptor.version(),
                        descriptor.id(), descriptor.version());
                    descriptors.put(descriptor.id(), descriptor);
                }
            }
        }
        return ImmutableList.copyOf(descriptors.values());
    }
}
