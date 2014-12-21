package org.mitallast.queue.queue.transactional.mmap.meta;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class QueueMessageMeta {

    private final static AtomicReferenceFieldUpdater<QueueMessageMeta, QueueMessageStatus> statusUpdater =
            AtomicReferenceFieldUpdater.newUpdater(QueueMessageMeta.class, QueueMessageStatus.class, "status");
    private final UUID uuid;
    private final long offset;
    private final int length;
    private final int pos;
    private final int type;
    private volatile QueueMessageStatus status;

    public QueueMessageMeta(UUID uuid, QueueMessageStatus status, long offset, int length, int pos, int type) {
        this.uuid = uuid;
        this.status = status;
        this.offset = offset;
        this.length = length;
        this.pos = pos;
        this.type = type;
    }

    public boolean queue() {
        return statusUpdater.compareAndSet(this, QueueMessageStatus.INIT, QueueMessageStatus.QUEUED);
    }

    public boolean lock() {
        return statusUpdater.compareAndSet(this, QueueMessageStatus.QUEUED, QueueMessageStatus.LOCKED);
    }

    public boolean unlockAndQueue() {
        return statusUpdater.compareAndSet(this, QueueMessageStatus.LOCKED, QueueMessageStatus.QUEUED);
    }

    public boolean unlockAndDelete() {
        return statusUpdater.compareAndSet(this, QueueMessageStatus.LOCKED, QueueMessageStatus.DELETED);
    }

    public UUID getUuid() {
        return uuid;
    }

    public QueueMessageStatus getStatus() {
        return status;
    }

    public long getOffset() {
        return offset;
    }

    public int getLength() {
        return length;
    }

    public int getPos() {
        return pos;
    }

    public int getType() {
        return type;
    }

    @Override
    @SuppressWarnings("RedundantIfStatement")
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueueMessageMeta that = (QueueMessageMeta) o;

        if (length != that.length) return false;
        if (offset != that.offset) return false;
        if (pos != that.pos) return false;
        if (type != that.type) return false;
        if (uuid != null ? !uuid.equals(that.uuid) : that.uuid != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = uuid != null ? uuid.hashCode() : 0;
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        result = 31 * result + length;
        result = 31 * result + pos;
        result = 31 * result + type;
        return result;
    }

    @Override
    public String toString() {
        return "QueueMessageMeta{" +
                "status=" + status +
                ", uuid=" + uuid +
                ", offset=" + offset +
                ", length=" + length +
                ", pos=" + pos +
                ", type=" + type +
                '}';
    }
}
