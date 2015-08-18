package org.mitallast.queue.log;

import io.netty.buffer.ByteBuf;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.mmap.MemoryMappedFileBuffer;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.log.entry.LogEntry;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public class Segment implements Closeable {
    private final File file;
    private final SegmentDescriptor descriptor;
    private final MemoryMappedFileBuffer fileBuffer;
    private final ByteBuf buffer;
    private final StreamInput streamInput;
    private final StreamOutput streamOutput;
    private final SegmentIndex offsetIndex;
    private int skip = 0;
    private boolean closed;

    public Segment(File file, SegmentDescriptor descriptor, SegmentIndex offsetIndex, StreamService streamService) throws IOException {
        this.file = file;
        this.descriptor = descriptor;
        this.fileBuffer = new MemoryMappedFileBuffer(file, descriptor.maxSegmentSize());
        this.buffer = fileBuffer.buffer();
        this.offsetIndex = offsetIndex;
        this.streamInput = streamService.input(buffer);
        this.streamOutput = streamService.output(buffer);

        buffer.writerIndex((int) offsetIndex.nextPosition());
    }

    public File file() {
        return file;
    }

    public SegmentDescriptor descriptor() {
        return descriptor;
    }

    public boolean isEmpty() {
        return offsetIndex.size() == 0;
    }

    public boolean isFull() throws IOException {
        if (closed) {
            throw new IllegalStateException("Segment is closed " + descriptor.index() + ":" + descriptor.version());
        }
        return size() >= descriptor.maxSegmentSize()
            || offsetIndex.size() >= descriptor.maxEntries();
    }

    /**
     * @return size in bytes
     */
    public long size() {
        checkClosed();
        return buffer.writerIndex();
    }

    /**
     * @return log entries count
     */
    public int length() {
        return offsetIndex.size();
    }

    public long firstIndex() {
        return !isEmpty() ? descriptor.index() : 0;
    }

    public long lastIndex() {
        return !isEmpty() ? offsetIndex.lastOffset() + descriptor.index() + skip : 0;
    }

    public long nextIndex() {
        return !isEmpty() ? lastIndex() + 1 : descriptor.index() + skip;
    }

    private int offset(long index) {
        return (int) (index - descriptor.index());
    }

    private void checkRange(long index) {
        if (isEmpty())
            throw new IndexOutOfBoundsException("segment is empty");
        if (index < firstIndex())
            throw new IndexOutOfBoundsException(index + " is less than the first index in the segment");
        if (index > lastIndex())
            throw new IndexOutOfBoundsException(index + " is greater than the last index in the segment");
    }

    public long appendEntry(LogEntry entry) throws IOException {
        checkClosed();
        if (isFull()) {
            throw new IllegalStateException("Segment is full " + descriptor.index() + ":" + descriptor.version());
        }
        long index = nextIndex();
        if (entry.index() != index) {
            throw new IndexOutOfBoundsException("inconsistent index: " + entry.index() + " entry: " + entry);
        }

        // Calculate the offset of the entry.
        int offset = offset(index);
        int start = buffer.writerIndex();

        // Record the starting position of the new entry.
        // Serialize the object into the segment buffer.
        EntryBuilder entryBuilder = entry.toBuilder();
        streamOutput.writeClass(entryBuilder.getClass());
        streamOutput.writeStreamable(entryBuilder);
        // flush();

        int end = buffer.writerIndex();
        offsetIndex.index(offset, start, end - start, MessageStatus.QUEUED);

        // Reset skip to zero since we wrote a new entry.
        skip = 0;
        return index;
    }

    public <T extends LogEntry> T getEntry(long index) throws IOException {
        checkClosed();
        checkRange(index);

        // Get the offset of the index within this segment.
        int offset = offset(index);

        // Get the start position of the offset from the offset index.
        long position = offsetIndex.position(offset);

        // If the position is -1 then that indicates no start position was found. The offset may have been removed from
        // the index via deduplication or compaction.
        if (position != -1) {
            // Deserialize the entry from a slice of the underlying buffer.
            buffer.readerIndex((int) position);
            LogEntry.Builder<?, T> entryBuilder = streamInput.readStreamable();
            entryBuilder.setIndex(index);
            return entryBuilder.build();
        }
        return null;
    }

    public boolean containsIndex(long index) {
        checkClosed();
        return !isEmpty() && index >= descriptor.index() && index <= lastIndex();
    }

    public boolean containsEntry(long index) throws IOException {
        checkClosed();
        return containsIndex(index) && offsetIndex.contains(offset(index));
    }

    public Segment skip(long entries) {
        checkClosed();
        this.skip += entries;
        return this;
    }

    public Segment truncate(long index) throws IOException {
        checkClosed();
        int offset = offset(index);
        if (offset < offsetIndex.lastOffset()) {
            int diff = (int) (offsetIndex.lastOffset() - offset);
            skip = Math.max(skip - diff, 0);
            offsetIndex.truncate(offset);
        }
        return this;
    }

    public synchronized Segment flush() throws IOException {
        checkClosed();
        fileBuffer.flush();
        offsetIndex.flush();
        return this;
    }

    private void checkClosed() {
        if (closed) {
            throw new IllegalStateException("Segment is closed " + descriptor.index() + ":" + descriptor.version());
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (!closed) {
            flush();
            closed = true;
            fileBuffer.close();
            offsetIndex.close();
        }
    }
}
