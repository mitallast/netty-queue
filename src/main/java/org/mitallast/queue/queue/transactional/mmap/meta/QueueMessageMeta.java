package org.mitallast.queue.queue.transactional.mmap.meta;

import org.mitallast.queue.queue.QueueMessageType;

import java.util.UUID;

public class QueueMessageMeta {

    private final UUID uuid;
    private final long offset;
    private final int length;
    private final QueueMessageType type;
    private final QueueMessageStatus status;

    public QueueMessageMeta(UUID uuid, QueueMessageStatus status, long offset, int length, QueueMessageType type) {
        this.uuid = uuid;
        this.status = status;
        this.offset = offset;
        this.length = length;
        this.type = type;
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

    public QueueMessageType getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueueMessageMeta that = (QueueMessageMeta) o;

        if (length != that.length) return false;
        if (offset != that.offset) return false;
        if (type != that.type) return false;
        if (!uuid.equals(that.uuid)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = uuid.hashCode();
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        result = 31 * result + length;
        result = 31 * result + type.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "QueueMessageMeta{" +
                "status=" + status +
                ", uuid=" + uuid +
                ", offset=" + offset +
                ", length=" + length +
                ", type=" + type +
                '}';
    }
}
