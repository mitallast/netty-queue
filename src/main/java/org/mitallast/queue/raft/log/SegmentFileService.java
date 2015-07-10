package org.mitallast.queue.raft.log;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;

import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SegmentFileService extends AbstractComponent {

    private final static Pattern descriptorPattern = Pattern.compile("^log-(\\d+)-(\\d+)\\.[a-z]+");
    private final File directory;

    @Inject
    public SegmentFileService(Settings settings) throws IOException {
        super(settings);

        File workDir = new File(this.settings.get("work_dir", "data"));
        directory = new File(workDir, componentSettings.get("log_dir", "log"));
        if (!directory.exists()) {
            if (!directory.mkdir()) {
                throw new IOException("Error create directory: " + directory);
            }
        }
    }

    public ImmutableList<SegmentFile> listDescriptorFiles() {
        return listFiles(".descriptor");
    }

    public ImmutableList<SegmentFile> listFiles(String extension) {
        ImmutableList.Builder<SegmentFile> builder = ImmutableList.builder();
        for (File file : directory.listFiles(File::isFile)) {
            if (file.getName().endsWith(extension)) {
                Matcher matcher = descriptorPattern.matcher(file.getName());
                if (matcher.matches()) {
                    long id = Long.parseLong(matcher.group(1), 10);
                    long version = Long.parseLong(matcher.group(2), 10);
                    builder.add(new SegmentFile(file, id, version));
                }
            }
        }
        return builder.build();
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
