package org.mitallast.queue.queue.transactional.mmap.meta;

import gnu.trove.impl.HashFunctions;
import gnu.trove.impl.PrimeFinder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.mitallast.queue.common.Locks;
import org.mitallast.queue.common.mmap.MemoryMappedFile;
import org.mitallast.queue.queue.QueueMessageStatus;
import org.mitallast.queue.queue.QueueMessageType;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.mitallast.queue.queue.QueueMessageStatus.*;

public class MMapQueueMessageMetaSegment implements QueueMessageMetaSegment {

    public final static int DEFAULT_MAX_SIZE = 1048576;
    public final static float DEFAULT_LOAD_FACTOR = 0.7f;

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
    private final AtomicInteger sizeCounter;
    private final int maxSize;

    public MMapQueueMessageMetaSegment(MemoryMappedFile mappedFile, int maxSize, float loadFactor) throws IOException {
        this.mappedFile = mappedFile;
        this.sizeCounter = new AtomicInteger();
        this.maxSize = maxSize;
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

        init();
    }

    public MemoryMappedFile getMappedFile() {
        return mappedFile;
    }

    private void init() throws IOException {
        if (mappedFile.isEmpty()) return;
        for (int pos = 0; pos < size; pos++) {
            QueueMessageMeta meta = readMeta(pos);
            if (meta != null) {
                uuidMap.set(pos, meta.getUuid());
                statusMap.set(pos, meta.getStatus());
                sizeCounter.incrementAndGet();
            }
        }
    }

    private boolean incrementSize() {
        while (true) {
            int currentSize = sizeCounter.get();
            if (currentSize < maxSize) {
                if (sizeCounter.compareAndSet(currentSize, currentSize + 1)) {
                    return true;
                } else {
                    Locks.parkNanos();
                }
            } else {
                return false;
            }
        }
    }

    @Override
    public QueueMessageMeta peek() throws IOException {
        for (int index = 0; index < size; index++) {
            if (statusMap.get(index) == QUEUED) {
                return readMeta(index);
            }
        }
        return null;
    }

    @Override
    public QueueMessageMeta lockAndPop() throws IOException {
        for (int index = 0; index < size; index++) {
            if (setStatusLocked(index)) {
                return readMeta(index);
            }
        }
        return null;
    }

    @Override
    public QueueMessageMeta lock(UUID uuid) throws IOException {
        final int index = index(uuid);
        if (index >= 0) {
            if (setStatusLocked(index)) {
                return readMeta(index);
            }
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

    private boolean setStatusInit(int index) {
        while (true) {
            QueueMessageStatus current = statusMap.get(index);
            if (current == null) {
                if (statusMap.compareAndSet(index, null, QueueMessageStatus.INIT)) {
                    return true;
                }
            } else {
                return false;
            }
        }
    }

    private boolean setStatusLocked(int index) {
        while (true) {
            QueueMessageStatus current = statusMap.get(index);
            if (current == QUEUED) {
                if (statusMap.compareAndSet(index, current, QueueMessageStatus.LOCKED)) {
                    return true;
                }
            } else {
                return false;
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
                return false;
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
    public boolean insert(UUID uuid) throws IOException {
        return incrementSize() && insertKey(uuid) >= 0;
    }

    @Override
    public boolean writeLock(UUID uuid) throws IOException {
        int index = insertKey(uuid);
        return index >= 0 && setStatusInit(index);
    }

    /**
     * @return -1 if null, pos if equals, else -2 if not equals
     */
    private int comparePosition(UUID uuid, int pos) {
        UUID actual = uuidMap.get(pos);
        if (actual == null) return -1;
        return uuid.equals(actual) ? pos : -2;
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
        final int index = insertKey(meta.getUuid());
        if (index >= 0) {
            if (statusMap.compareAndSet(index, INIT, LOCKED)) {
                writeMeta(meta, index);
                statusMap.set(index, QUEUED);
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isGarbage() throws IOException {
        int deleted = 0;
        for (int i = 0; i < size; i++) {
            QueueMessageStatus status = statusMap.get(i);
            if (status != null) {
                if (DELETED != status) {
                    return false;
                }
                deleted++;
            }
        }
        return deleted == maxSize;
    }

    @Override
    public int size() {
        return sizeCounter.get();
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

        int cmp = comparePosition(uuid, index);
        if (cmp != -2) {
            return cmp;
        }

        final int loopIndex = index;
        final int probe = 1 + (hash % (size - 2));
        do {
            index = index - probe;
            if (index < 0) {
                index += size;
            }

            cmp = comparePosition(uuid, index);
            if (cmp != -2) {
                return cmp;
            }
            // Detect loop
        } while (index != loopIndex);
        return -1;
    }

    private int insertKey(final UUID uuid) {
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

    @Override
    public void close() throws IOException {
        mappedFile.flush();
        mappedFile.close();
    }

    @Override
    public void delete() throws IOException {
        mappedFile.delete();
    }
}
