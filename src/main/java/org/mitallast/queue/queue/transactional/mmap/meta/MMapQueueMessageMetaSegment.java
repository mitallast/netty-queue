package org.mitallast.queue.queue.transactional.mmap.meta;

import gnu.trove.impl.HashFunctions;
import gnu.trove.impl.PrimeFinder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.mitallast.queue.common.mmap.MemoryMappedFile;
import org.mitallast.queue.queue.QueueMessageType;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.mitallast.queue.queue.transactional.mmap.meta.QueueMessageStatus.DELETED;
import static org.mitallast.queue.queue.transactional.mmap.meta.QueueMessageStatus.QUEUED;

public class MMapQueueMessageMetaSegment implements QueueMessageMetaSegment {

    private final static long INT_SIZE = 4;
    private final static long LONG_SIZE = 8;
    private final static long MESSAGE_META_SIZE = LONG_SIZE * 8;
    private final static long MESSAGE_COUNT_OFFSET = 0;
    private final static long MESSAGE_META_OFFSET = MESSAGE_COUNT_OFFSET + INT_SIZE;

    private final MemoryMappedFile mappedFile;
    private final int size;
    private final AtomicReferenceArray<UUID> uuidMap;
    private final AtomicReferenceArray<QueueMessageStatus> statusMap;
    private final ThreadLocal<ByteBuf> localBuffer;

    public MMapQueueMessageMetaSegment(MemoryMappedFile mappedFile, int maxSize, float loadFactor) {
        this.mappedFile = mappedFile;

        int ceil = HashFunctions.fastCeil(maxSize / loadFactor);
        size = PrimeFinder.nextPrime(ceil);
        uuidMap = new AtomicReferenceArray<>(size);
        statusMap = new AtomicReferenceArray<>(size);
        localBuffer = new ThreadLocal<ByteBuf>() {
            @Override
            protected ByteBuf initialValue() {
                return Unpooled.buffer(512);
            }
        };
    }

    @Override
    public QueueMessageMeta lock(UUID uuid) throws IOException {
        final int index = index(uuid);
        if (index >= 0) {
            setStatusLocked(index);
            return readMeta(index);
        }
        return null;
    }

    @Override
    public QueueMessageMeta unlockAndDelete(UUID uuid) throws IOException {
        final int index = index(uuid);
        if (index >= 0) {
            boolean deleted = setStatusDeleted(index);
            QueueMessageMeta meta = readMeta(index);
            if (deleted) {
                writeMeta(meta);
            }
            return meta;
        }
        return null;
    }

    @Override
    public QueueMessageMeta unlockAndQueue(UUID uuid) throws IOException {
        final int index = index(uuid);
        if (index >= 0) {
            boolean queued = setStatusQueued(index);
            QueueMessageMeta meta = readMeta(index);
            if (queued) {
                writeMeta(meta);
            }
            return meta;
        }
        return null;
    }

    private boolean setStatusLocked(int index) {
        while (true) {
            QueueMessageStatus current = statusMap.get(index);
            if (current == null || current == QUEUED) {
                if (statusMap.compareAndSet(index, current, QueueMessageStatus.LOCKED)) {
                    return true;
                }
            } else {
                return true;
            }
        }
    }

    private boolean setStatusDeleted(int index) {
        while (true) {
            QueueMessageStatus current = statusMap.get(index);
            if (current == QueueMessageStatus.LOCKED) {
                if (statusMap.compareAndSet(index, current, DELETED)) {
                    return true;
                }
            } else {
                return true;
            }
        }
    }

    private boolean setStatusQueued(int index) {
        while (true) {
            QueueMessageStatus current = statusMap.get(index);
            if (current == null || current == QueueMessageStatus.LOCKED) {
                if (statusMap.compareAndSet(index, current, QUEUED)) {
                    return true;
                }
            } else {
                return true;
            }
        }
    }

    @Override
    public boolean writeLock(UUID uuid) throws IOException {
        return insert(uuid) >= 0;
    }

    private boolean comparePosition(UUID uuid, int pos) {
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

    @Override
    public QueueMessageMeta readMeta(UUID uuid) throws IOException {
        final int index = index(uuid);
        if (index >= 0) {
            return readMeta(index);
        }
        return null;
    }

    private QueueMessageMeta readMeta(int pos) throws IOException {
        long metaOffset = getMetaOffset(pos);
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
        QueueMessageStatus statusSaved = QueueMessageStatus.values()[buffer.readInt()];
        QueueMessageStatus status = statusMap.get(pos);
        if (status == null) {
            status = statusSaved;
        }
        long offset = buffer.readLong();
        int length = buffer.readInt();
        int type = buffer.readInt();
        return new QueueMessageMeta(uuid, status, offset, length, QueueMessageType.values()[type]);
    }

    @Override
    public boolean writeMeta(QueueMessageMeta meta) throws IOException {
        final int index = insert(meta.getUuid());
        if (index >= 0) {
            writeMeta(meta, index);
            return true;
        }
        return false;
    }

    private void writeMeta(QueueMessageMeta messageMeta, int pos) throws IOException {
        long offset = getMetaOffset(pos);
        ByteBuf buffer = localBuffer.get();
        buffer.clear();
        buffer.clear();
        buffer.writeLong(messageMeta.getUuid().getMostSignificantBits());
        buffer.writeLong(messageMeta.getUuid().getLeastSignificantBits());

        switch (messageMeta.getStatus()) {
            case LOCKED:
                // do not write lock status
                buffer.writeInt(QUEUED.ordinal());
            case INIT:
            case QUEUED:
            case DELETED:
                buffer.writeInt(messageMeta.getStatus().ordinal());
                break;
            default:
                throw new IOException("Unexpected status: " + messageMeta.getStatus());
        }

        buffer.writeLong(messageMeta.getOffset());
        buffer.writeInt(messageMeta.getLength());
        buffer.writeInt(messageMeta.getType().ordinal());
        buffer.resetReaderIndex();
        mappedFile.putBytes(offset, buffer);
    }

    private int index(final UUID uuid) {
        final int hash = uuid.hashCode() & 0x7fffffff;
        int index = hash % size;

        if (comparePosition(uuid, index)) {
            return index;
        }

        final int loopIndex = index;
        final int probe = 1 + (hash % (size - 2));
        do {
            index = index - probe;
            if (index < 0) {
                index += size;
            }

            if (comparePosition(uuid, index)) {
                return index;
            }
            // Detect loop
        } while (index != loopIndex);
        return -1;
    }

    private int insert(final UUID uuid) {
        final int hash = uuid.hashCode() & 0x7fffffff;
        int index = hash % size;

        if (lockPosition(uuid, index)) {
            return index;
        }

        final int loopIndex = index;
        final int probe = 1 + (hash % (size - 2));
        do {
            index = index - probe;
            if (index < 0) {
                index += size;
            }

            if (lockPosition(uuid, index)) {
                return index;
            }
            // Detect loop
        } while (index != loopIndex);
        return -1;
    }

    private long getMetaOffset(int pos) {
        return MESSAGE_META_OFFSET + MESSAGE_META_SIZE * pos;
    }
}
