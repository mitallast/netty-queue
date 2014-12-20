package org.mitallast.queue.queue.service;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.mitallast.queue.common.mmap.MemoryMappedFile;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.QueueMessageType;
import org.mitallast.queue.queue.QueueMessageUuidDuplicateException;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * File structure
 * - int message count
 * - message meta info * size
 * - message data * size
 */
public class FileQueueMessageAppendTransactionLog implements Closeable {

    private final static AtomicReferenceFieldUpdater<FileQueueMessageAppendTransactionLog, QueueMessageMeta> tailUpdater =
            AtomicReferenceFieldUpdater.newUpdater(FileQueueMessageAppendTransactionLog.class, QueueMessageMeta.class, "tail");
    private final static AtomicReferenceFieldUpdater<FileQueueMessageAppendTransactionLog, QueueMessageMeta> headUpdater =
            AtomicReferenceFieldUpdater.newUpdater(FileQueueMessageAppendTransactionLog.class, QueueMessageMeta.class, "head");

    private final static long INT_SIZE = 4;
    private final static long LONG_SIZE = 8;
    private final static long MESSAGE_META_SIZE = LONG_SIZE * 8;
    private final static long MESSAGE_COUNT_OFFSET = 0;
    private final static long MESSAGE_META_OFFSET = MESSAGE_COUNT_OFFSET + INT_SIZE;
    private final MemoryMappedFile metaMemoryMappedFile;
    private final MemoryMappedFile dataMemoryMappedFile;

    private final AtomicLong messageWriteOffset = new AtomicLong();
    private final AtomicInteger messageCount = new AtomicInteger();
    private final ConcurrentHashMap<UUID, QueueMessageMeta> messageMetaMap = new ConcurrentHashMap<>(65536, 0.5f);
    private transient volatile QueueMessageMeta head;
    private transient volatile QueueMessageMeta tail;

    public FileQueueMessageAppendTransactionLog(File metaFile, File dataFile) throws IOException {
        this(metaFile, dataFile, 1048576, 10);
    }

    public FileQueueMessageAppendTransactionLog(File metaFile, File dataFile, int pageSize, int maxPages) throws IOException {
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
        metaMemoryMappedFile = new MemoryMappedFile(new RandomAccessFile(metaFile, "rw"), pageSize, maxPages);
        dataMemoryMappedFile = new MemoryMappedFile(new RandomAccessFile(dataFile, "rw"), pageSize, maxPages);

        head = tail = QueueMessageMeta.empty();
    }

    private boolean offer(final QueueMessageMeta newNode) {
        assert newNode != null;

        for (QueueMessageMeta t = tail, p = t; ; ) {
            QueueMessageMeta q = p.next;
            if (q == null) {
                // p is last node
                if (p.casNext(null, newNode)) {
                    // Successful CAS is the linearization point
                    // for e to become an element of this queue,
                    // and for newNode to become "live".
                    if (p != t) // hop two nodes at a time
                        casTail(t, newNode);  // Failure is OK.
                    return true;
                }
                // Lost CAS race to another thread; re-read next
            } else if (p == q)
                // We have fallen off list.  If tail is unchanged, it
                // will also be off-list, in which case we need to
                // jump to head, from which all live nodes are always
                // reachable.  Else the new tail is a better bet.
                p = (t != (t = tail)) ? t : head;
            else
                // Check for tail updates after two hops.
                p = (p != t && t != (t = tail)) ? t : q;
        }
    }

    private QueueMessageMeta poll() {
        restartFromHead:
        for (; ; ) {
            for (QueueMessageMeta h = head, p = h, q; ; ) {
                if (p.updateStatus(QueueMessageMeta.Status.New, QueueMessageMeta.Status.Deleted)) {
                    // Successful CAS is the linearization point
                    // for item to be removed from this queue.
                    if (p != h) // hop two nodes at a time
                        updateHead(h, ((q = p.next) != null) ? q : p);
                    return p;
                } else if ((q = p.next) == null) {
                    updateHead(h, p);
                    return null;
                } else if (p == q)
                    continue restartFromHead;
                else
                    p = q;
            }
        }
    }

    private QueueMessageMeta peek() {
        restartFromHead:
        for (; ; ) {
            for (QueueMessageMeta h = head, p = h, q; ; ) {
                boolean hasItem = p.isStatus(QueueMessageMeta.Status.New);
                if (hasItem || (q = p.next) == null) {
                    updateHead(h, p);
                    return hasItem ? p : null;
                } else if (p == q)
                    continue restartFromHead;
                else
                    p = q;
            }
        }
    }

    private void updateHead(QueueMessageMeta h, QueueMessageMeta p) {
        if (h != p && casHead(h, p))
            h.lazySetNext(h);
    }

    private boolean casTail(QueueMessageMeta cmp, QueueMessageMeta val) {
        return tailUpdater.compareAndSet(this, cmp, val);
    }

    private boolean casHead(QueueMessageMeta cmp, QueueMessageMeta val) {
        return headUpdater.compareAndSet(this, cmp, val);
    }

    public void initializeNew() throws IOException {
        writeMessageCount(0);
        head = tail = QueueMessageMeta.empty();

        messageMetaMap.clear();
        messageWriteOffset.set(0);
    }

    public void initializeExists() throws IOException {
        messageCount.set(readMessageCount());
        messageMetaMap.clear();
        head = tail = QueueMessageMeta.empty();

        for (int i = 0; i < messageCount.get(); i++) {
            QueueMessageMeta messageMeta = readMeta(i);
            messageMeta.pos = i;
            messageMetaMap.put(messageMeta.uuid, messageMeta);
            offer(messageMeta);
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
        QueueMessageMeta messageMeta = QueueMessageMeta.empty();
        messageMeta.uuid = queueMessage.getUuid();
        messageMeta.pos = messageCount.getAndIncrement();
        if (messageMetaMap.putIfAbsent(messageMeta.uuid, messageMeta) != null) {
            throw new QueueMessageUuidDuplicateException(messageMeta.uuid);
        }

        ByteBuf source = queueMessage.getSource();
        source.resetReaderIndex();

        messageMeta.offset = messageWriteOffset.getAndAdd(source.readableBytes());
        messageMeta.length = source.readableBytes();
        messageMeta.type = queueMessage.getMessageType().ordinal();
        messageMeta.setStatus(QueueMessageMeta.Status.New);

        dataMemoryMappedFile.putBytes(messageMeta.offset, source);
//        dataMemoryMappedFile.flush();

        writeMeta(messageMeta, messageMeta.pos);
        writeMessageCount(messageCount.get());

        offer(messageMeta);

//        metaMemoryMappedFile.flush();
        return messageMeta.pos;
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
        QueueMessageMeta messageMeta = peek();
        if (messageMeta != null) {
            return readMessage(messageMeta, false);
        }
        return null;
    }

    public QueueMessage dequeueMessage() throws IOException {
        QueueMessageMeta messageMeta = poll();
        if (messageMeta != null) {
            messageMetaMap.remove(messageMeta.uuid);
            writeMeta(messageMeta, messageMeta.pos);
            return readMessage(messageMeta, false);
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
        ByteBuf buffer = Unpooled.buffer(meta.length);
        dataMemoryMappedFile.getBytes(meta.offset, buffer, meta.length);
        return new QueueMessage(meta.uuid, QueueMessageType.values()[meta.type], buffer);
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
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer((int) MESSAGE_META_SIZE);
        buffer.clear();
        try {
            buffer.writeLong(messageMeta.uuid.getMostSignificantBits());
            buffer.writeLong(messageMeta.uuid.getLeastSignificantBits());
            buffer.writeLong(messageMeta.offset);
            buffer.writeInt(messageMeta.status);
            buffer.writeInt(messageMeta.length);
            buffer.writeInt(messageMeta.type);
            buffer.resetReaderIndex();
            metaMemoryMappedFile.putBytes(offset, buffer);
        } finally {
            buffer.release();
        }
    }

    private QueueMessageMeta readMeta(int pos) throws IOException {
        QueueMessageMeta messageMeta = readMeta(getMetaOffset(pos));
        messageMeta.pos = pos;
        return messageMeta;
    }

    public QueueMessageMeta readMeta(long offset) throws IOException {
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer((int) MESSAGE_META_SIZE);
        buffer.clear();
        try {
            metaMemoryMappedFile.getBytes(offset, buffer, (int) MESSAGE_META_SIZE);
            buffer.resetReaderIndex();

            QueueMessageMeta messageMeta = QueueMessageMeta.empty();
            long UUIDMost = buffer.readLong();
            long UUIDLeast = buffer.readLong();
            if (UUIDMost != 0 && UUIDLeast != 0) {
                messageMeta.uuid = new UUID(UUIDMost, UUIDLeast);
            } else {
                messageMeta.uuid = null;
            }
            messageMeta.offset = buffer.readLong();
            messageMeta.status = buffer.readInt();
            messageMeta.length = buffer.readInt();
            messageMeta.type = buffer.readInt();
            return messageMeta;
        } finally {
            buffer.release();
        }
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

        private final static AtomicReferenceFieldUpdater<QueueMessageMeta, QueueMessageMeta> nextUpdater =
                AtomicReferenceFieldUpdater.newUpdater(QueueMessageMeta.class, QueueMessageMeta.class, "next");

        UUID uuid;
        long offset;
        volatile int status;
        int length;
        int pos;
        int type;

        volatile QueueMessageMeta next;

        QueueMessageMeta(UUID uuid, long offset, int status, int length, int type) {
            this.uuid = uuid;
            this.offset = offset;
            this.status = status;
            this.length = length;
            this.type = type;
        }

        static QueueMessageMeta empty() {
            return new QueueMessageMeta(null, -1, Status.Deleted.ordinal(), -1, -1);
        }

        boolean isStatus(Status expectedStatus) {
            return expectedStatus.ordinal() == status;
        }

        void setStatus(Status newStatus) {
            status = newStatus.ordinal();
        }

        boolean updateStatus(Status expectedStatus, Status newStatus) {
            return statusUpdater.compareAndSet(this, expectedStatus.ordinal(), newStatus.ordinal());
        }

        void lazySetNext(QueueMessageMeta val) {
            nextUpdater.lazySet(this, val);
        }

        boolean casNext(QueueMessageMeta cmp, QueueMessageMeta val) {
            return nextUpdater.compareAndSet(this, cmp, val);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            QueueMessageMeta that = (QueueMessageMeta) o;

            if (length != that.length) return false;
            if (offset != that.offset) return false;
            if (status != that.status) return false;
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
                    "uuid=" + uuid +
                    ", offset=" + offset +
                    ", status=" + status +
                    ", length=" + length +
                    ", type=" + type +
                    '}';
        }

        enum Status {None, New, Deleted}
    }
}
