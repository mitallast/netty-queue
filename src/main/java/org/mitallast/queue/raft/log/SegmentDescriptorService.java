package org.mitallast.queue.raft.log;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.common.unit.ByteSizeUnit;
import org.mitallast.queue.common.unit.ByteSizeValue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SegmentDescriptorService extends AbstractComponent {

    private final SegmentFileService fileService;
    private final StreamService streamService;

    private final long maxEntrySize;
    private final long maxSegmentSize;
    private final long maxEntries;

    @Inject
    public SegmentDescriptorService(Settings settings, SegmentFileService fileService, StreamService streamService) {
        super(settings);
        this.fileService = fileService;
        this.streamService = streamService;

        maxEntrySize = componentSettings.getAsBytesSize("max_entry_size", new ByteSizeValue(100, ByteSizeUnit.MB)).bytes();
        maxSegmentSize = componentSettings.getAsBytesSize("max_segment_size", new ByteSizeValue(100, ByteSizeUnit.MB)).bytes();
        maxEntries = componentSettings.getAsLong("max_entries_per_segment", 1000000l);
    }

    public long getMaxEntrySize() {
        return maxEntrySize;
    }

    public long getMaxSegmentSize() {
        return maxSegmentSize;
    }

    public long getMaxEntries() {
        return maxEntries;
    }

    public SegmentDescriptor createDescriptor(long id, long index, long version) throws IOException {
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

    public SegmentDescriptor readDescriptor(File file) throws IOException {
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

    public void deleteDescriptor(SegmentDescriptor descriptor) throws IOException {
        logger.info("delete segment {}:{}", descriptor.id(), descriptor.version());
        File file = fileService.createDescriptorFile(descriptor);
        if (file.exists()) {
            assert file.delete();
        }
    }

    public ImmutableList<SegmentDescriptor> loadDescriptors() throws IOException {
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
