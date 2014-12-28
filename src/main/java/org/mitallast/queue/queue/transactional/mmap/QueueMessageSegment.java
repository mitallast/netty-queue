package org.mitallast.queue.queue.transactional.mmap;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.QueueMessageType;
import org.mitallast.queue.queue.transactional.mmap.data.QueueMessageAppendSegment;
import org.mitallast.queue.queue.transactional.mmap.meta.QueueMessageMeta;
import org.mitallast.queue.queue.transactional.mmap.meta.QueueMessageMetaSegment;
import org.mitallast.queue.queue.transactional.mmap.meta.QueueMessageStatus;

import java.io.IOException;
import java.util.UUID;

public class QueueMessageSegment {
    private final QueueMessageAppendSegment messageAppendSegment;
    private final QueueMessageMetaSegment messageMetaSegment;

    public QueueMessageSegment(QueueMessageAppendSegment messageAppendSegment, QueueMessageMetaSegment messageMetaSegment) {
        this.messageAppendSegment = messageAppendSegment;
        this.messageMetaSegment = messageMetaSegment;
    }

    public boolean writeMessage(QueueMessage queueMessage) throws IOException {
        if (messageMetaSegment.writeLock(queueMessage.getUuid())) {
            ByteBuf source = queueMessage.getSource();
            source.resetReaderIndex();
            int length = source.readableBytes();
            long offset = messageAppendSegment.append(source);

            QueueMessageMeta messageMeta = new QueueMessageMeta(
                    queueMessage.getUuid(),
                    QueueMessageStatus.INIT,
                    offset,
                    length,
                    0,
                    queueMessage.getMessageType().ordinal()
            );

            messageMetaSegment.writeMeta(messageMeta);
            return true;
        }
        return false;
    }

    public QueueMessage readMessage(UUID uuid) throws IOException {
        QueueMessageMeta meta = messageMetaSegment.readMeta(uuid);
        if (meta != null) {
            ByteBuf buffer = Unpooled.buffer(meta.getLength());
            messageAppendSegment.read(buffer, meta.getOffset(), meta.getLength());
            return new QueueMessage(
                    uuid,
                    QueueMessageType.values()[meta.getType()],
                    buffer
            );
        }
        return null;
    }
}
