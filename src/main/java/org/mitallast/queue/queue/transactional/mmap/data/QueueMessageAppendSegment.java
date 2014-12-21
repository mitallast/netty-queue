package org.mitallast.queue.queue.transactional.mmap.data;

import io.netty.buffer.ByteBuf;
import org.mitallast.queue.common.mmap.MemoryMappedFile;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class QueueMessageAppendSegment {

    private final MemoryMappedFile mappedFile;
    private final AtomicLong messageWriteOffset;

    public QueueMessageAppendSegment(MemoryMappedFile mappedFile) {
        this.mappedFile = mappedFile;
        this.messageWriteOffset = new AtomicLong();
    }

    public void read(ByteBuf buffer, long offset, int length) throws IOException {
        mappedFile.getBytes(offset, buffer, length);
    }

    public long append(ByteBuf buffer) throws IOException {
        long offset = messageWriteOffset.getAndAdd(buffer.readableBytes());
        mappedFile.putBytes(offset, buffer);
        return offset;
    }
}