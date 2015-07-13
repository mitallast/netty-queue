package org.mitallast.queue.raft.log;

import com.google.inject.Inject;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.log.Log;
import org.mitallast.queue.log.LogService;
import org.mitallast.queue.log.SegmentManager;
import org.mitallast.queue.raft.log.entry.RaftLogEntry;

import java.io.IOException;

public class RaftLog extends AbstractComponent {

    private final Log log;

    @Inject
    public RaftLog(Settings settings, LogService logService) throws IOException {
        super(settings);
        this.log = logService.openLog("raft");
    }

    public RaftLog(Settings settings, Log log) {
        super(settings);
        this.log = log;
    }

    public SegmentManager segmentManager() {
        return log.segmentManager();
    }

    public boolean isEmpty() {
        return log.isEmpty();
    }

    public long size() {
        return log.size();
    }

    public long length() {
        return log.length();
    }

    public long firstIndex() {
        return log.firstIndex();
    }

    public long nextIndex() {
        return log.nextIndex();
    }

    public long lastIndex() {
        return log.lastIndex();
    }

    public long appendEntry(RaftLogEntry entry) throws IOException {
        return log.appendEntry(entry);
    }

    public <T extends RaftLogEntry> T getEntry(long index) throws IOException {
        return log.getEntry(index);
    }

    public boolean containsIndex(long index) {
        return log.containsIndex(index);
    }

    public boolean containsEntry(long index) throws IOException {
        return log.containsEntry(index);
    }

    public RaftLog skip(long entries) throws IOException {
        log.skip(entries);
        return this;
    }

    public RaftLog truncate(long index) throws IOException {
        log.truncate(index);
        return this;
    }

    public void flush() throws IOException {
        log.flush();
    }

    public void delete() throws IOException {
        log.delete();
    }
}
