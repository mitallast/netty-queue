package org.mitallast.queue.queue.transactional;

import org.mitallast.queue.queue.QueueMessage;

import java.io.IOException;
import java.util.UUID;

public interface TransactionalQueueComponent {

    public QueueMessage get(UUID uuid) throws IOException;

    public QueueMessage lock(UUID uuid) throws IOException;

    public QueueMessage lockAndPop() throws IOException;

    public QueueMessage unlockAndDelete(UUID uuid) throws IOException;

    public QueueMessage unlockAndRollback(UUID uuid) throws IOException;

    long size();
}
