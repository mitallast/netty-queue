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

    private void checkIndex(long index) {
        if (!containsIndex(index))
            throw new IndexOutOfBoundsException(index + " is not a valid log index");
    }

    public boolean isEmpty() {
        return segmentManager.firstSegment().isEmpty();
    }

    public long size() {
        return segmentManager.segments().stream().mapToLong(Segment::size).sum();
    }

    public long length() {
        return segmentManager.segments().stream().mapToLong(Segment::length).sum();
    }

    public long firstIndex() {
        return !isEmpty() ? segmentManager.firstSegment().descriptor().index() : 0;
    }

    public long nextIndex() {
        return segmentManager.currentSegment().nextIndex();
    }

    public long lastIndex() {
        return !isEmpty() ? segmentManager.lastSegment().lastIndex() : 0;
    }

    private void checkRoll() throws IOException {
        if (segmentManager.currentSegment().isFull()) {
            segmentManager.nextSegment();
        }
    }

    public long appendEntry(LogEntry entry) throws IOException {
        checkRoll();
        return segmentManager.currentSegment().appendEntry(entry);
    }

    public <T extends LogEntry> T getEntry(long index) throws IOException {
        checkIndex(index);
        Segment segment = segmentManager.segment(index);
        if (segment == null)
            throw new IndexOutOfBoundsException("invalid index: " + index);
        return segment.getEntry(index);
    }

    public boolean containsIndex(long index) {
        return !isEmpty() && firstIndex() <= index && index <= lastIndex();
    }

    public boolean containsEntry(long index) throws IOException {
        if (!containsIndex(index))
            return false;
        Segment segment = segmentManager.segment(index);
        return segment != null && segment.containsEntry(index);
    }

    public Log skip(long entries) throws IOException {
        Segment segment = segmentManager.currentSegment();
        while (segment.length() + entries > Integer.MAX_VALUE) {
            int skip = Integer.MAX_VALUE - segment.length();
            segment.skip(skip);
            entries -= skip;
            segment = segmentManager.nextSegment();
        }
        segment.skip(entries);
        return this;
    }

    public Log truncate(long index) throws IOException {
        checkIndex(index);
        if (lastIndex() == index)
            return this;

        for (Segment segment : segmentManager.segments()) {
            if (segment.containsIndex(index)) {
                segment.truncate(index);
            } else if (segment.descriptor().index() > index) {
                segmentManager.remove(segment);
            }
        }
        return this;
    }

    public void flush() throws IOException {
        segmentManager.currentSegment().flush();
    }

    public void delete() throws IOException {
        segmentManager.delete();
    }
}

