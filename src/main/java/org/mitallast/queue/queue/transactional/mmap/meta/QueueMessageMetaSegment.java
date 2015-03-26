package org.mitallast.queue.queue.transactional.mmap.meta;

import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;

public interface QueueMessageMetaSegment extends Closeable {

    QueueMessageMeta peek() throws IOException;

    QueueMessageMeta lockAndPop() throws IOException;

    QueueMessageMeta lock(UUID uuid) throws IOException;

    QueueMessageMeta unlockAndDelete(UUID uuid) throws IOException;

    QueueMessageMeta unlockAndQueue(UUID uuid) throws IOException;

    boolean insert(UUID uuid) throws IOException;

    boolean writeLock(UUID uuid) throws IOException;

    boolean writeMeta(QueueMessageMeta meta) throws IOException;

    QueueMessageMeta readMeta(UUID uuid) throws IOException;

    boolean isGarbage() throws IOException;

    int size();

    void delete() throws IOException;
}
