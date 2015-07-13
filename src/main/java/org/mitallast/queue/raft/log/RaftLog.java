package org.mitallast.queue.raft.log;

import com.google.inject.Inject;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.raft.log.entry.LogEntry;

import java.io.IOException;

public class RaftLog extends AbstractComponent {

    protected final SegmentManager segments;

    @Inject
    public RaftLog(Settings settings, SegmentManager segments) {
        super(settings);
        this.segments = segments;
    }

    private void checkIndex(long index) {
        if (!containsIndex(index))
            throw new IndexOutOfBoundsException(index + " is not a valid log index");
    }

    public boolean isEmpty() {
        return segments.firstSegment().isEmpty();
    }

    public long size() {
        return segments.segments().stream().mapToLong(Segment::size).sum();
    }

    public long length() {
        return segments.segments().stream().mapToLong(Segment::length).sum();
    }

    public long firstIndex() {
        return !isEmpty() ? segments.firstSegment().descriptor().index() : 0;
    }

    public long nextIndex() {
        return segments.currentSegment().nextIndex();
    }

    public long lastIndex() {
        return !isEmpty() ? segments.lastSegment().lastIndex() : 0;
    }

    private void checkRoll() throws IOException {
        if (segments.currentSegment().isFull()) {
            segments.nextSegment();
        }
    }

    public long appendEntry(LogEntry entry) throws IOException {
        checkRoll();
        return segments.currentSegment().appendEntry(entry);
    }

    public <T extends LogEntry> T getEntry(long index) throws IOException {
        checkIndex(index);
        Segment segment = segments.segment(index);
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
        Segment segment = segments.segment(index);
        return segment != null && segment.containsEntry(index);
    }

    public RaftLog skip(long entries) throws IOException {
        Segment segment = segments.currentSegment();
        while (segment.length() + entries > Integer.MAX_VALUE) {
            int skip = Integer.MAX_VALUE - segment.length();
            segment.skip(skip);
            entries -= skip;
            segment = segments.nextSegment();
        }
        segment.skip(entries);
        return this;
    }

    public RaftLog truncate(long index) throws IOException {
        checkIndex(index);
        if (lastIndex() == index)
            return this;

        for (Segment segment : segments.segments()) {
            if (segment.containsIndex(index)) {
                segment.truncate(index);
            } else if (segment.descriptor().index() > index) {
                segments.remove(segment);
            }
        }
        return this;
    }

    public void flush() throws IOException {
        segments.currentSegment().flush();
    }

    public void delete() throws IOException {
        segments.delete();
    }
}
