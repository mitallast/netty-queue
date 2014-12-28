package org.mitallast.queue.queue.transactional.mmap.data;

import io.netty.buffer.ByteBuf;
import org.mitallast.queue.common.mmap.MemoryMappedFile;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class MMapQueueMessageAppendSegment implements QueueMessageAppendSegment {

    private final MemoryMappedFile mappedFile;
    private final AtomicLong messageWriteOffset;

    public MMapQueueMessageAppendSegment(MemoryMappedFile mappedFile) {
        this.mappedFile = mappedFile;
        this.messageWriteOffset = new AtomicLong();
    }

    @Override
    public void read(ByteBuf buffer, long offset, int length) throws IOException {
        mappedFile.getBytes(offset, buffer, length);
    }

    @Override
    public long append(ByteBuf buffer) throws IOException {
        long offset = messageWriteOffset.getAndAdd(buffer.readableBytes());
        mappedFile.putBytes(offset, buffer);
        return offset;
    }
}