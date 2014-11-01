package org.mitallast.queue.queue.service.translog;

import org.mitallast.queue.queue.QueueMessage;

import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;

public interface QueueMessageAppendTransactionLog extends Closeable {

    void initializeNew() throws IOException;

    void initializeExists() throws IOException;

    int putMessage(QueueMessage queueMessage) throws IOException;

    void markMessageDeleted(UUID uuid) throws IOException;

    QueueMessage readMessage(UUID uuid) throws IOException;

    QueueMessage readMessage(UUID uuid, boolean checkDeletion) throws IOException;

    QueueMessage peekMessage() throws IOException;

    QueueMessage dequeueMessage() throws IOException;
}
