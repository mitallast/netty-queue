package org.mitallast.queue.queue.transactional.mmap;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.QueueMessageStatus;
import org.mitallast.queue.queue.transactional.mmap.data.MMapQueueMessageAppendSegment;
import org.mitallast.queue.queue.transactional.mmap.meta.MMapQueueMessageMetaSegment;
import org.mitallast.queue.queue.transactional.mmap.meta.QueueMessageMeta;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class MMapQueueMessageSegment implements TransactionalQueueSegment {

    private final MMapQueueMessageAppendSegment messageAppendSegment;
    private final MMapQueueMessageMetaSegment messageMetaSegment;
    private final AtomicInteger referenceCount;

    public MMapQueueMessageSegment(MMapQueueMessageAppendSegment messageAppendSegment, MMapQueueMessageMetaSegment messageMetaSegment) {
        this.messageAppendSegment = messageAppendSegment;
        this.messageMetaSegment = messageMetaSegment;
        this.referenceCount = new AtomicInteger();
    }

    public MMapQueueMessageAppendSegment getMessageAppendSegment() {
        return messageAppendSegment;
    }

    public MMapQueueMessageMetaSegment getMessageMetaSegment() {
        return messageMetaSegment;
    }

    public int acquire() {
        while (true) {
            int current = referenceCount.get();
            if (current >= 0) {
                int next = current + 1;
                if (referenceCount.compareAndSet(current, next)) {
                    return next;
                }
            } else return current;
        }
    }

    public int release() {
        return referenceCount.decrementAndGet();
    }

    public boolean releaseGarbage() {
        return referenceCount.compareAndSet(0, -1);
    }

    private QueueMessage readMessage(QueueMessageMeta meta) throws IOException {
        ByteBuf buffer = Unpooled.buffer(meta.getLength());
        messageAppendSegment.read(buffer, meta.getOffset(), meta.getLength());
        return new QueueMessage(
            meta.getUuid(),
            meta.getType(),
            buffer
        );
    }

    @Override
    public QueueMessage get(UUID uuid) throws IOException {
        QueueMessageMeta meta = messageMetaSegment.readMeta(uuid);
        if (meta != null) {
            return readMessage(meta);
        }
        return null;
    }

    @Override
    public int insert(UUID uuid) throws IOException {
        return messageMetaSegment.insert(uuid);
    }

    @Override
    public boolean writeLock(int pos) throws IOException {
        return messageMetaSegment.writeLock(pos);
    }

    @Override
    public boolean writeMessage(QueueMessage queueMessage, int pos) throws IOException {
        ByteBuf source = queueMessage.getSource();
        source.resetReaderIndex();
        int length = source.readableBytes();
        long offset = messageAppendSegment.append(source);

        QueueMessageMeta messageMeta = new QueueMessageMeta(
            queueMessage.getUuid(),
            QueueMessageStatus.QUEUED,
            offset,
            length,
            queueMessage.getMessageType()
        );

        messageMetaSegment.writeMeta(messageMeta, pos);
        return true;
    }

    @Override
    public QueueMessage lock(UUID uuid) throws IOException {
        QueueMessageMeta meta = messageMetaSegment.lock(uuid);
        if (meta != null) {
            return readMessage(meta);
        }
        return null;
    }

    @Override
    public QueueMessage peek() throws IOException {
        QueueMessageMeta meta = messageMetaSegment.peek();
        if (meta != null) {
            return readMessage(meta);
        }
        return null;
    }

    @Override
    public QueueMessage lockAndPop() throws IOException {
        QueueMessageMeta meta = messageMetaSegment.lockAndPop();
        if (meta != null) {
            return readMessage(meta);
        }
        return null;
    }

    @Override
    public QueueMessage unlockAndDelete(UUID uuid) throws IOException {
        QueueMessageMeta meta = messageMetaSegment.unlockAndDelete(uuid);
        if (meta != null) {
            return readMessage(meta);
        }
        return null;
    }

    @Override
    public QueueMessage unlockAndRollback(UUID uuid) throws IOException {
        QueueMessageMeta meta = messageMetaSegment.unlockAndQueue(uuid);
        if (meta != null) {
            return readMessage(meta);
        }
        return null;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public void close() throws IOException {
        messageAppendSegment.close();
        messageMetaSegment.close();
    }

    @Override
    public boolean isGarbage() throws IOException {
        return messageMetaSegment.isGarbage();
    }

    @Override
    public void delete() throws IOException {
        messageAppendSegment.delete();
        messageMetaSegment.delete();
    }
}
