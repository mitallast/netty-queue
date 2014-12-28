package org.mitallast.queue.queue.transactional.mmap.meta;

import gnu.trove.impl.HashFunctions;
import gnu.trove.impl.PrimeFinder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.mitallast.queue.common.mmap.MemoryMappedFile;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class MMapQueueMessageMetaSegment implements QueueMessageMetaSegment {

    private final static long INT_SIZE = 4;
    private final static long LONG_SIZE = 8;
    private final static long MESSAGE_META_SIZE = LONG_SIZE * 8;
    private final static long MESSAGE_COUNT_OFFSET = 0;
    private final static long MESSAGE_META_OFFSET = MESSAGE_COUNT_OFFSET + INT_SIZE;

    private final MemoryMappedFile mappedFile;
    private final int size;
    private final AtomicReferenceArray<UUID> uuidMap;
    private final ThreadLocal<ByteBuf> localBuffer;

    public MMapQueueMessageMetaSegment(MemoryMappedFile mappedFile, int maxSize, float loadFactor) {
        this.mappedFile = mappedFile;

        int ceil = HashFunctions.fastCeil(maxSize / loadFactor);
        size = PrimeFinder.nextPrime(ceil);
        uuidMap = new AtomicReferenceArray<>(size);
        localBuffer = new ThreadLocal<ByteBuf>() {
            @Override
            protected ByteBuf initialValue() {
                return Unpooled.buffer(512);
            }
        };
    }

    @Override
    public boolean writeLock(UUID uuid) throws IOException {
        final int hash = uuid.hashCode() & 0x7fffffff;
        int index = hash % size;

        if (lockPosition(uuid, index)) {
            return true;
        }

        final int loopIndex = index;
        final int probe = 1 + (hash % (size - 2));
        do {
            index = index - probe;
            if (index < 0) {
                index += size;
            }

            if (lockPosition(uuid, index)) {
                return true;
            }
            // Detect loop
        } while (index != loopIndex);
        return false;
    }

    @Override
    public boolean writeMeta(QueueMessageMeta meta) throws IOException {
        final UUID uuid = meta.getUuid();
        final int hash = uuid.hashCode() & 0x7fffffff;
        int index = hash % size;

        if (lockPosition(uuid, index)) {
            writeMeta(meta, index);
            return true;
        }

        final int loopIndex = index;
        final int probe = 1 + (hash % (size - 2));
        do {
            index = index - probe;
            if (index < 0) {
                index += size;
            }

            if (lockPosition(uuid, index)) {
                writeMeta(meta, index);
                return true;
            }
            // Detect loop
        } while (index != loopIndex);
        return false;
    }

    @Override
    public QueueMessageMeta readMeta(UUID uuid) throws IOException {
        final int hash = uuid.hashCode() & 0x7fffffff;
        int index = hash % size;

        if (comparePosition(uuid, index)) {
            return readMeta(index);
        }

        final int loopIndex = index;
        final int probe = 1 + (hash % (size - 2));
        do {
            index = index - probe;
            if (index < 0) {
                index += size;
            }
            if (comparePosition(uuid, index)) {
                return readMeta(index);
            }
            // Detect loop
        } while (index != loopIndex);
        return null;
    }

    private boolean comparePosition(UUID uuid, int pos) throws IOException {
        return uuid.equals(uuidMap.get(pos));
    }

    private boolean lockPosition(UUID uuid, int pos) {
        while (true) {
            UUID actual = uuidMap.get(pos);
            if (actual == null) {
                if (uuidMap.compareAndSet(pos, null, uuid)) {
                    return true;
                }
            } else {
                return actual.equals(uuid);
            }
        }
    }

    private QueueMessageMeta readMeta(int pos) throws IOException {
        return readMeta(getMetaOffset(pos));
    }

    private QueueMessageMeta readMeta(long metaOffset) throws IOException {
        ByteBuf buffer = localBuffer.get();
        buffer.clear();
        mappedFile.getBytes(metaOffset, buffer, (int) MESSAGE_META_SIZE);
        buffer.resetReaderIndex();
        UUID uuid;
        long UUIDMost = buffer.readLong();
        long UUIDLeast = buffer.readLong();
        if (UUIDMost != 0 && UUIDLeast != 0) {
            uuid = new UUID(UUIDMost, UUIDLeast);
        } else {
            // does not read entry without uuid
            return null;
        }
        QueueMessageStatus status = QueueMessageStatus.values()[buffer.readInt()];
        long offset = buffer.readLong();
        int length = buffer.readInt();
        int pos = buffer.readInt();
        int type = buffer.readInt();
        return new QueueMessageMeta(uuid, status, offset, length, pos, type);
    }

    private void writeMeta(QueueMessageMeta messageMeta, int pos) throws IOException {
        writeMeta(messageMeta, getMetaOffset(pos));
    }

    private void writeMeta(QueueMessageMeta messageMeta, long offset) throws IOException {
        ByteBuf buffer = localBuffer.get();
        buffer.clear();
        buffer.clear();
        buffer.writeLong(messageMeta.getUuid().getMostSignificantBits());
        buffer.writeLong(messageMeta.getUuid().getLeastSignificantBits());
        buffer.writeInt(messageMeta.getStatus().ordinal());
        buffer.writeLong(messageMeta.getOffset());
        buffer.writeInt(messageMeta.getLength());
        buffer.writeInt(messageMeta.getPos());
        buffer.writeInt(messageMeta.getType());
        buffer.resetReaderIndex();
        mappedFile.putBytes(offset, buffer);
    }

    private long getMetaOffset(int pos) {
        return MESSAGE_META_OFFSET + MESSAGE_META_SIZE * pos;
    }
}
