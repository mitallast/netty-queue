package org.mitallast.queue.common.mmap;

import org.mitallast.queue.common.UUIDs;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;

import java.io.File;
import java.io.IOException;

public class MemoryMappedFileFactory extends AbstractComponent {

    private final File directory;
    private final int pageSize;
    private final int maxPages;

    public MemoryMappedFileFactory(Settings settings, File directory) throws IOException {
        super(settings);
        this.directory = directory;
        pageSize = this.settings.getAsInt("page_size", 65536);
        maxPages = this.settings.getAsInt("max_pages", 10);
        if (!directory.exists()) {
            if (!directory.mkdir()) {
                throw new IOException("Error create directory " + directory);
            }
        }
    }

    public MemoryMappedFile createFile(String ext) throws IOException {
        String fileName = UUIDs.generateRandom().toString() + '.' + ext;
        File file = new File(directory, fileName);
        if (!file.createNewFile()) {
            throw new IOException("Error create new file " + file);
        }

        return new MemoryMappedFile(file, pageSize, maxPages);
    }
}
