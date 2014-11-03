package org.mitallast.queue.queue.service.translog;

import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.QueueMessageUuidDuplicateException;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;

/**
 * File structure
 * - int message count
 * - message meta info * size
 * - message data * size
 */
public class FileQueueMessageAppendTransactionLog implements Closeable {

    private final static long INT_SIZE = 4;
    private final static long LONG_SIZE = 8;
    private final static long MESSAGE_META_SIZE = LONG_SIZE * 3 + INT_SIZE * 2;
    private final static long MESSAGE_COUNT_OFFSET = 0;
    private final static long MESSAGE_META_OFFSET = MESSAGE_COUNT_OFFSET + INT_SIZE;
    private final MemoryMappedFile metaMemoryMappedFile;
    private final MemoryMappedFile dataMemoryMappedFile;

    private final AtomicLong messageWriteOffset = new AtomicLong();
    private final AtomicInteger messageCount = new AtomicInteger();

    private final ConcurrentLinkedQueue<QueueMessageMeta> messageMetaQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentHashMap<UUID, QueueMessageMeta> messageMetaMap = new ConcurrentHashMap<>(256);

    public FileQueueMessageAppendTransactionLog(File metaFile, File dataFile) throws IOException {
        if (!metaFile.exists()) {
            if (!metaFile.createNewFile()) {
                throw new IOException("File not found, or not writable " + metaFile);
            }
        }
        if (!dataFile.exists()) {
            if (!dataFile.createNewFile()) {
                throw new IOException("File not found, or not writable " + metaFile);
            }
        }
        metaMemoryMappedFile = new MemoryMappedFile(new RandomAccessFile(metaFile, "rw"), 4096, 10);
        dataMemoryMappedFile = new MemoryMappedFile(new RandomAccessFile(dataFile, "rw"), 4096, 10);
    }

    public void initializeNew() throws IOException {
        writeMessageCount(0);
        messageMetaQueue.clear();
        messageMetaMap.clear();
        messageWriteOffset.set(0);
    }

    public void initializeExists() throws IOException {
        messageCount.set(readMessageCount());
        messageMetaMap.clear();
        messageMetaQueue.clear();

        for (int i = 0; i < messageCount.get(); i++) {
            QueueMessageMeta messageMeta = readMeta(i);
            messageMeta.pos = i;
            messageMetaMap.put(messageMeta.uuid, messageMeta);
            messageMetaQueue.add(messageMeta);
        }
        messageWriteOffset.set(dataMemoryMappedFile.length());
    }

    public int getMessageCount() {
        return messageCount.get();
    }

    /**
     * @param queueMessage new message
     * @return message position
     */
    public int putMessage(QueueMessage queueMessage) throws IOException {
        int pos = messageCount.getAndIncrement();

        QueueMessageMeta messageMeta = new QueueMessageMeta();
        messageMeta.uuid = queueMessage.getUuid();
        messageMeta.pos = pos;
        if (messageMetaMap.putIfAbsent(messageMeta.uuid, messageMeta) != null) {
            throw new QueueMessageUuidDuplicateException(messageMeta.uuid);
        }

        byte[] source = queueMessage.getSource();

        messageMeta.offset = messageWriteOffset.getAndAdd(source.length);

        messageMeta.length = source.length;
        messageMeta.setStatus(QueueMessageMeta.Status.New);

        dataMemoryMappedFile.putBytes(messageMeta.offset, source);
//        dataMemoryMappedFile.flush();

        writeMeta(messageMeta, messageMeta.pos);
        writeMessageCount(messageCount.get());

        messageMetaQueue.add(messageMeta);

//        metaMemoryMappedFile.flush();
        return pos;
    }

    public void markMessageDeleted(UUID uuid) throws IOException {
        QueueMessageMeta meta = messageMetaMap.get(uuid);
        if (meta != null) {
            meta.setStatus(QueueMessageMeta.Status.Deleted);
            writeMeta(meta, meta.pos);
            messageMetaMap.remove(uuid);
        }
    }

    public QueueMessage peekMessage() throws IOException {
        for (QueueMessageMeta messageMeta : messageMetaQueue) {
            if (messageMeta.isStatus(QueueMessageMeta.Status.New)) {
                return readMessage(messageMeta, false);
            }
        }
        return null;
    }

    public QueueMessage dequeueMessage() throws IOException {
        QueueMessageMeta messageMeta;
        while ((messageMeta = messageMetaQueue.poll()) != null) {
            if (messageMeta.updateStatus(QueueMessageMeta.Status.New, QueueMessageMeta.Status.Deleted)) {
                writeMeta(messageMeta, messageMeta.pos);
                messageMetaMap.remove(messageMeta.uuid);
                return readMessage(messageMeta, false);
            }
        }
        return null;
    }

    public QueueMessage readMessage(UUID uuid) throws IOException {
        QueueMessageMeta meta = messageMetaMap.get(uuid);
        if (meta != null) {
            return readMessage(meta, true);
        }
        return null;
    }

    private QueueMessage readMessage(QueueMessageMeta meta, boolean checkDeletion) throws IOException {
        if (meta.isStatus(QueueMessageMeta.Status.None)) {
            return null;
        }
        if (checkDeletion && meta.isStatus(QueueMessageMeta.Status.Deleted)) {
            return null;
        }
        byte[] source = new byte[meta.length];
        dataMemoryMappedFile.getBytes(meta.offset, source);
        return new QueueMessage(meta.uuid, source);
    }

    private void writeMessageCount(int maxSize) throws IOException {
        metaMemoryMappedFile.putInt(MESSAGE_COUNT_OFFSET, maxSize);
    }

    private int readMessageCount() throws IOException {
        return metaMemoryMappedFile.getInt(MESSAGE_COUNT_OFFSET);
    }

    public void writeMeta(QueueMessageMeta messageMeta, int pos) throws IOException {
        writeMeta(messageMeta, getMetaOffset(pos));
    }

    public void writeMeta(QueueMessageMeta messageMeta, long offset) throws IOException {
        if (messageMeta.uuid == null) {
            metaMemoryMappedFile.putLong(offset, 0);
            offset += LONG_SIZE;
            metaMemoryMappedFile.putLong(offset, 0);
            offset += LONG_SIZE;
        } else {
            metaMemoryMappedFile.putLong(offset, messageMeta.uuid.getMostSignificantBits());
            offset += LONG_SIZE;
            metaMemoryMappedFile.putLong(offset, messageMeta.uuid.getLeastSignificantBits());
            offset += LONG_SIZE;
        }
        metaMemoryMappedFile.putLong(offset, messageMeta.offset);
        offset += LONG_SIZE;
        metaMemoryMappedFile.putInt(offset, messageMeta.status);
        offset += INT_SIZE;
        metaMemoryMappedFile.putInt(offset, messageMeta.length);
    }

    private QueueMessageMeta readMeta(int pos) throws IOException {
        return readMeta(getMetaOffset(pos));
    }

    public QueueMessageMeta readMeta(long offset) throws IOException {
        QueueMessageMeta messageMeta = new QueueMessageMeta();
        long UUIDMost = metaMemoryMappedFile.getLong(offset);
        offset += LONG_SIZE;
        long UUIDLeast = metaMemoryMappedFile.getLong(offset);
        offset += LONG_SIZE;
        if (UUIDMost != 0 && UUIDLeast != 0) {
            messageMeta.uuid = new UUID(UUIDMost, UUIDLeast);
        } else {
            messageMeta.uuid = null;
        }
        messageMeta.offset = metaMemoryMappedFile.getLong(offset);
        offset += LONG_SIZE;
        messageMeta.status = metaMemoryMappedFile.getInt(offset);
        offset += INT_SIZE;
        messageMeta.length = metaMemoryMappedFile.getInt(offset);
        return messageMeta;
    }

    private long getMetaOffset(int pos) {
        return MESSAGE_META_OFFSET + MESSAGE_META_SIZE * pos;
    }

    @Override
    public void close() throws IOException {
        metaMemoryMappedFile.close();
        dataMemoryMappedFile.close();
    }

    public static class QueueMessageMeta {

        private final static AtomicIntegerFieldUpdater<QueueMessageMeta> statusUpdater =
                AtomicIntegerFieldUpdater.newUpdater(QueueMessageMeta.class, "status");

        private UUID uuid;
        private long offset;
        private volatile int status;
        private int length;
        private int pos;

        public QueueMessageMeta() {
        }

        public QueueMessageMeta(UUID uuid, long offset, int status, int length) {
            this.uuid = uuid;
            this.offset = offset;
            this.status = status;
            this.length = length;
        }

        public boolean isStatus(Status expectedStatus) {
            return expectedStatus.ordinal() == status;
        }

        public void setStatus(Status newStatus) {
            status = newStatus.ordinal();
        }

        public boolean updateStatus(Status expectedStatus, Status newStatus) {
            return statusUpdater.compareAndSet(this, expectedStatus.ordinal(), newStatus.ordinal());
        }

        @Override
        public String toString() {
            return "QueueMessageMeta{" +
                    ", uuid=" + uuid +
                    ", offset=" + offset +
                    ", status=" + status +
                    ", length=" + length +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            QueueMessageMeta that = (QueueMessageMeta) o;

            if (length != that.length) return false;
            if (offset != that.offset) return false;
            if (status != that.status) return false;
            if (uuid != null ? !uuid.equals(that.uuid) : that.uuid != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return uuid != null ? uuid.hashCode() : 0;
        }

        enum Status {None, New, Deleted}
    }
}
