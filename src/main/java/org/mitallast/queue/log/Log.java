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
        if (!(!isEmpty() && firstIndex() <= index && index <= lastIndex())) {
            throw new IndexOutOfBoundsException(index + " is not a valid log index");
        }
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
        while (segment.length() + entries > Integer.MAX_VALUE) {
            int skip = Integer.MAX_VALUE - segment.length();
            segment.skip(skip);
            entries -= skip;
            segment = segmentManager.nextSegment();
        }
        segment.skip(entries);
    }

    public void truncate(long index) throws IOException {
        checkIndex(index);
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

