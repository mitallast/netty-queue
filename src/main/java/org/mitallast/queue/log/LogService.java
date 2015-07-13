package org.mitallast.queue.log;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.stream.StreamService;

import java.io.Closeable;
import java.io.IOException;

public class LogService extends AbstractLifecycleComponent {

    private final StreamService streamService;
    private ImmutableMap<String, LogRegistry> registryMap = ImmutableMap.of();

    @Inject
    public LogService(Settings settings, StreamService streamService) {
        super(settings);
        this.streamService = streamService;
    }

    public synchronized Log log(String name) throws IOException {
        LogRegistry registry = registryMap.get(name);
        if (registry == null) {
            registry = new LogRegistry(name);
            registryMap = ImmutableMap.<String, LogRegistry>builder()
                .putAll(registryMap)
                .put(name, registry)
                .build();
        }
        return registry.log;
    }

    @Override
    protected void doStart() throws IOException {

    }

    @Override
    protected void doStop() throws IOException {

    }

    @Override
    protected void doClose() throws IOException {
        for (LogRegistry registry : registryMap.values()) {
            try {
                registry.close();
            } catch (IOException e) {
                logger.error("error close registry {}", registry.logSettings.get("log.name"));
            }
        }
    }

    private class LogRegistry implements Closeable {
        private final String name;
        private final Settings logSettings;
        private final SegmentFileService fileService;
        private final SegmentDescriptorService descriptorService;
        private final SegmentIndexService indexService;
        private final SegmentService segmentService;
        private final SegmentManager segmentManager;
        private final Log log;

        public LogRegistry(String name) throws IOException {
            this.name = name;
            logSettings = ImmutableSettings.builder()
                .put(settings)
                .put("log.name", name)
                .build();
            logger.info("open log {}", name);
            fileService = new SegmentFileService(logSettings);
            descriptorService = new SegmentDescriptorService(logSettings, fileService, streamService);
            indexService = new SegmentIndexService(logSettings, fileService);
            segmentService = new SegmentService(logSettings, streamService, fileService, indexService);
            segmentManager = new SegmentManager(logSettings, descriptorService, segmentService);
            segmentManager.start();
            log = new Log(logSettings, segmentManager);
            logger.info("open log {} done", name);
        }

        @Override
        public void close() throws IOException {
            logger.info("close log {}", name);
            segmentManager.stop();
            segmentManager.close();
        }
    }
}
