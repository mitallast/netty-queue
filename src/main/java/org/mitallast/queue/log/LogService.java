package org.mitallast.queue.log;

import com.google.inject.Inject;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.stream.StreamService;

import java.io.IOException;

public class LogService extends AbstractComponent {

    private final StreamService streamService;

    @Inject
    public LogService(Settings settings, StreamService streamService) {
        super(settings);
        this.streamService = streamService;
    }

    public Log openLog(String name) throws IOException {
        logger.info("open log {}", name);
        Settings logSettings = ImmutableSettings.builder()
            .put(settings)
            .put("log.name", name)
            .build();
        SegmentFileService fileService = new SegmentFileService(logSettings);
        SegmentDescriptorService descriptorService = new SegmentDescriptorService(logSettings, fileService, streamService);
        SegmentIndexService indexService = new SegmentIndexService(logSettings, fileService);
        SegmentService segmentService = new SegmentService(logSettings, streamService, fileService, indexService);
        SegmentManager segmentManager = new SegmentManager(logSettings, descriptorService, segmentService);
        segmentManager.start();
        Log log = new Log(logSettings, segmentManager);
        logger.info("open log {} done", name);
        return log;
    }
}
