package org.mitallast.queue.raft.log;

import java.io.File;

class SegmentFile {
    private final File file;

    SegmentFile(File file) {
        if (!isSegmentFile(file))
            throw new IllegalArgumentException("Not a valid segment file");
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

    public static boolean isSegmentFile(File file) {
        return isFile(file, "log");
    }

    public static boolean isIndexFile(File file) {
        return isFile(file, "index");
    }

    public static boolean isFile(File file, String extension) {
        return file.getName().indexOf('-') != -1
            && file.getName().indexOf('-', file.getName().indexOf('-') + 1) != -1
            && file.getName().lastIndexOf('.') > file.getName().lastIndexOf('-')
            && file.getName().endsWith("." + extension);
    }

    public static File createSegmentFile(File directory, long id, long version) {
        return new File(directory, String.format("log-%d-%d.log", id, version));
    }

    public static File createIndexFile(File directory, long id, long version) {
        return new File(directory, String.format("log-%d-%d.index", id, version));
    }

}
