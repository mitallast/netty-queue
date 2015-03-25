package org.mitallast.queue.queue.transactional.mmap;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.QueueMessageStatus;
import org.mitallast.queue.queue.transactional.mmap.data.QueueMessageAppendSegment;
import org.mitallast.queue.queue.transactional.mmap.meta.QueueMessageMeta;
import org.mitallast.queue.queue.transactional.mmap.meta.QueueMessageMetaSegment;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class QueueMessageSegment implements TransactionalQueueSegment {

    private final QueueMessageAppendSegment messageAppendSegment;
    private final QueueMessageMetaSegment messageMetaSegment;
    private final AtomicInteger referenceCount;

    public QueueMessageSegment(QueueMessageAppendSegment messageAppendSegment, QueueMessageMetaSegment messageMetaSegment) {
        this.messageAppendSegment = messageAppendSegment;
        this.messageMetaSegment = messageMetaSegment;
        this.referenceCount = new AtomicInteger();
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
    public boolean insert(UUID uuid) throws IOException {
        return messageMetaSegment.insert(uuid);
    }

    @Override
    public boolean writeLock(UUID uuid) throws IOException {
        return messageMetaSegment.writeLock(uuid);
    }

    @Override
    public boolean writeMessage(QueueMessage queueMessage) throws IOException {
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

        messageMetaSegment.writeMeta(messageMeta);
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
