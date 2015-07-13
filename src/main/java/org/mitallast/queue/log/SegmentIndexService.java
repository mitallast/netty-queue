package org.mitallast.queue.log;

import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;

import java.io.File;
import java.io.IOException;

public class SegmentIndexService extends AbstractComponent {

    private final SegmentFileService fileService;

    public SegmentIndexService(Settings settings, SegmentFileService fileService) {
        super(settings);
        this.fileService = fileService;
    }

    public SegmentIndex createIndex(SegmentDescriptor descriptor) throws IOException {
        File file = fileService.createIndexFile(descriptor);
        if (!file.exists()) {
            if (!file.createNewFile()) {
                throw new IOException("Error create io file");
            }
        }
        return new SegmentIndex(file, (int) descriptor.maxEntries());
    }

    public void deleteIndex(SegmentDescriptor descriptor) throws IOException {
        File file = fileService.createIndexFile(descriptor);
        if (file.exists()) {
            assert file.delete();
        }
    }
}
