package org.mitallast.queue.log;

import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.log.entry.LogEntry;

import java.io.IOException;

public class Log extends AbstractComponent {

    protected final SegmentManager segmentManager;

    public Log(Settings settings, SegmentManager segmentManager) {
        super(settings);
        this.segmentManager = segmentManager;
    }

    public SegmentManager segmentManager() {
        return segmentManager;
    }

    public boolean isEmpty() {
        return segmentManager.firstSegment().isEmpty();
    }

    public long size() {
        long size = 0;
        for (Segment segment : segmentManager.segments()) {
            size += segment.size();
        }
        return size;
    }

    public long length() {
        long length = 0;
        for (Segment segment : segmentManager.segments()) {
            length += segment.length();
        }
        return length;
    }

    public long firstIndex() {
        return segmentManager.firstSegment().descriptor().index();
    }

    public long nextIndex() {
        return segmentManager.currentSegment().nextIndex();
    }

    public long lastIndex() {
        return segmentManager.lastSegment().lastIndex();
    }

    public long appendEntry(LogEntry entry) throws IOException {
        while (true) {
            try {
                return segmentManager.currentSegment().appendEntry(entry);
            } catch (IndexOutOfBoundsException e) {
                logger.debug("index is full");
                segmentManager.nextSegment();
            }
        }
    }

    public <T extends LogEntry> T getEntry(long index) throws IOException {
        Segment segment = segmentManager.segment(index);
        if (segment == null)
            return null;
        return segment.getEntry(index);
    }

    public boolean containsEntry(long index) throws IOException {
        Segment segment = segmentManager.segment(index);
        return segment != null && segment.containsEntry(index);
    }

    public void skip(long entries) throws IOException {
        Segment segment = segmentManager.currentSegment();
        while (entries > 0) {
            entries -= segment.skip(entries);
            segment = segmentManager.nextSegment();
        }
    }

    public void truncate(long index) throws IOException {
        if (!(!isEmpty() && firstIndex() <= index && index <= lastIndex())) {
            throw new IndexOutOfBoundsException(index + " is not a valid log index");
        }
        if (lastIndex() == index)
            return;

        for (Segment segment : segmentManager.segments()) {
            if (segment.containsIndex(index)) {
                segment.truncate(index);
            } else if (segment.descriptor().index() > index) {
                segmentManager.remove(segment);
                break; // next segments already removed
            }
        }
    }

    public void flush() throws IOException {
        segmentManager.currentSegment().flush();
    }

    public void delete() throws IOException {
        segmentManager.delete();
    }
}

