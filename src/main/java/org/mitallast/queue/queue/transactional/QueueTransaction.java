package org.mitallast.queue.queue.transactional;

import org.mitallast.queue.queue.QueueMessage;

import java.io.IOException;
import java.util.UUID;

public interface QueueTransaction {

    public UUID id() throws IOException;

    public void commit() throws IOException;

    public void rollback() throws IOException;

    public void push(QueueMessage queueMessage) throws IOException;

    public QueueMessage pop() throws IOException;

    public QueueMessage delete(UUID uuid) throws IOException;
}
