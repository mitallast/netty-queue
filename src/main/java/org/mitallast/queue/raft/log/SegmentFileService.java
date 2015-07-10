package org.mitallast.queue.raft.log;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;

import java.io.File;

public class SegmentFileService extends AbstractComponent {

    private final File directory;

    @Inject
    public SegmentFileService(Settings settings) {
        super(settings);

        File workDir = new File(this.settings.get("work_dir", "data"));
        directory = new File(workDir, componentSettings.get("log_dir", "log"));
    }

    public ImmutableList<File> listFiles() {
        return ImmutableList.copyOf(directory.listFiles(File::isFile));
    }

    public boolean isSegmentFile(File file) {
        return isFile(file, "log");
    }

    public boolean isIndexFile(File file) {
        return isFile(file, "index");
    }

    public boolean isDescriptorFile(File file) {
        return isFile(file, "info");
    }

    public boolean isFile(File file, String extension) {
        return file.getName().indexOf('-') != -1
            && file.getName().indexOf('-', file.getName().indexOf('-') + 1) != -1
            && file.getName().lastIndexOf('.') > file.getName().lastIndexOf('-')
            && file.getName().endsWith("." + extension);
    }

    public File createSegmentFile(SegmentDescriptor descriptor) {
        return new File(directory, String.format("log-%d-%d.log", descriptor.id(), descriptor.version()));
    }

    public File createIndexFile(SegmentDescriptor descriptor) {
        return new File(directory, String.format("log-%d-%d.index", descriptor.id(), descriptor.version()));
    }

    public File createDescriptorFile(SegmentDescriptor descriptor) {
        return new File(directory, String.format("log-%d-%d.info", descriptor.id(), descriptor.version()));
    }
}
