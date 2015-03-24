package org.mitallast.queue.queue.transactional.mmap;

import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.transactional.TransactionalQueueComponent;

import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;

public interface TransactionalQueueSegment extends TransactionalQueueComponent, Closeable {

    public boolean insert(UUID uuid) throws IOException;

    public boolean writeLock(UUID uuid) throws IOException;

    public boolean writeMessage(QueueMessage queueMessage) throws IOException;
}
