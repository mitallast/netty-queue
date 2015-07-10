package org.mitallast.queue.raft.log;

import java.io.File;

class SegmentFile {
    private final File file;
    private final long id;
    private final long version;

    public SegmentFile(File file, long id, long version) {
        this.file = file;
        this.id = id;
        this.version = version;
    }

    public File file() {
        return file;
    }

    public long id() {
        return id;
    }

    public long version() {
        return version;
    }
}
