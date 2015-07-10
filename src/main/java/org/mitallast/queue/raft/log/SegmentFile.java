package org.mitallast.queue.raft.log;

import java.io.File;

class SegmentFile {
    private final File file;

    public SegmentFile(File file) {
        this.file = file;
    }

    public File file() {
        return file;
    }

    public File index() {
        return new File(file.getParentFile(), file.getName().substring(0, file.getName().lastIndexOf('.') + 1) + "index");
    }

    public long id() {
        return Long.valueOf(file.getName().substring(file.getName().lastIndexOf('-', file.getName().lastIndexOf('-') - 1) + 1, file.getName().lastIndexOf('-')));
    }

    public long version() {
        return Long.valueOf(file.getName().substring(file.getName().lastIndexOf('-') + 1, file.getName().lastIndexOf('.')));
    }
}
