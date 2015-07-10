package org.mitallast.queue.raft.log;

import com.google.inject.Inject;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.stream.StreamService;

import java.io.File;
import java.io.IOException;

public class SegmentService extends AbstractComponent {
    private final StreamService streamService;
    private final SegmentFileService fileService;
    private final SegmentIndexService indexService;

    @Inject
    public SegmentService(Settings settings, StreamService streamService, SegmentFileService fileService, SegmentIndexService indexService) {
        super(settings);
        this.streamService = streamService;
        this.fileService = fileService;
        this.indexService = indexService;
    }

    public Segment createSegment(SegmentDescriptor descriptor) throws IOException {
        File segmentFile = fileService.createSegmentFile(descriptor);
        if (!segmentFile.exists()) {
            if (!segmentFile.createNewFile()) {
                throw new IOException("Error create new file: " + segmentFile);
            }
        }
        return new Segment(segmentFile, descriptor, indexService.createIndex(descriptor), streamService);
    }

    public void deleteSegment(SegmentDescriptor descriptor) throws IOException {
        indexService.deleteIndex(descriptor);
        File segmentFile = fileService.createSegmentFile(descriptor);
        if (segmentFile.exists()) {
            assert segmentFile.delete();
        }
    }
}
