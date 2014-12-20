package org.mitallast.queue.queue.transactional.mmap;

import gnu.trove.impl.HashFunctions;
import gnu.trove.impl.PrimeFinder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.mitallast.queue.common.concurrent.MapReentrantLock;
import org.mitallast.queue.common.mmap.MemoryMappedFile;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

public class QueueMessageMetaSegment {

    private final static long INT_SIZE = 4;
    private final static long LONG_SIZE = 8;
    private final static long MESSAGE_META_SIZE = LONG_SIZE * 8;
    private final static long MESSAGE_COUNT_OFFSET = 0;
    private final static long MESSAGE_META_OFFSET = MESSAGE_COUNT_OFFSET + INT_SIZE;

    private final MemoryMappedFile mappedFile;
    private final int size;
    private final MapReentrantLock locks;

    public QueueMessageMetaSegment(MemoryMappedFile mappedFile, int maxSize, float loadFactor) {
        this.mappedFile = mappedFile;

        int ceil = HashFunctions.fastCeil(maxSize / loadFactor);
        this.size = PrimeFinder.nextPrime(ceil);
        this.locks = new MapReentrantLock(128);
    }

    public boolean writeMeta(QueueMessageMeta newMeta) throws IOException {
        final int hash = newMeta.getUuid().hashCode() & 0x7fffffff;
        int index = hash % size;
        QueueMessageMeta saved;
        ReentrantLock lock;

        lock = locks.get(index);
        lock.lock();
        try {
            saved = readMeta(index);
            if (saved == null) {
                writeMeta(newMeta, index);
                return true;
            }
            if (saved.getUuid().equals(newMeta.getUuid())) {
                writeMeta(newMeta, index);
                return true;
            }
        } finally {
            lock.unlock();
        }

        final int loopIndex = index;
        final int probe = 1 + (hash % (size - 2));
        do {
            index = index - probe;
            if (index < 0) {
                index += size;
            }
            lock = locks.get(index);
            lock.lock();
            try {
                saved = readMeta(index);
                if (saved == null) {
                    writeMeta(newMeta, index);
                    return true;
                }
                if (saved.getUuid().equals(newMeta.getUuid())) {
                    writeMeta(newMeta, index);
                    return true;
                }
            } finally {
                lock.unlock();
            }
            // Detect loop
        } while (index != loopIndex);
        return false;
    }

    public QueueMessageMeta readMeta(UUID uuid) throws IOException {
        final int hash = uuid.hashCode() & 0x7fffffff;
        int index = hash % size;
        QueueMessageMeta meta;
        ReentrantLock lock;

        lock = locks.get(index);
        lock.lock();
        try {
            meta = readMeta(index);
        } finally {
            lock.unlock();
        }

        if (meta == null) {
            return null;
        }
        if (meta.getUuid().equals(uuid)) {
            return meta;
        }

        final int loopIndex = index;
        final int probe = 1 + (hash % (size - 2));
        do {
            index = index - probe;
            if (index < 0) {
                index += size;
            }
            lock = locks.get(index);
            lock.lock();
            try {
                meta = readMeta(index);
            } finally {
                lock.unlock();
            }
            if (meta == null) {
                return null;
            }
            if (meta.getUuid().equals(uuid)) {
                return meta;
            }
            // Detect loop
        } while (index != loopIndex);
        return null;
    }

    private QueueMessageMeta readMeta(int pos) throws IOException {
        return readMeta(getMetaOffset(pos));
    }

    private QueueMessageMeta readMeta(long metaOffset) throws IOException {
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer((int) MESSAGE_META_SIZE);
        try {
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
        } finally {
            buffer.release();
        }
    }

    private void writeMeta(QueueMessageMeta messageMeta, int pos) throws IOException {
        writeMeta(messageMeta, getMetaOffset(pos));
    }

    private void writeMeta(QueueMessageMeta messageMeta, long offset) throws IOException {
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer((int) MESSAGE_META_SIZE);
        try {
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
        } finally {
            buffer.release();
        }
    }

    private long getMetaOffset(int pos) {
        return MESSAGE_META_OFFSET + MESSAGE_META_SIZE * pos;
    }
}
