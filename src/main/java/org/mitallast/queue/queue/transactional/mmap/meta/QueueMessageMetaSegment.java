package org.mitallast.queue.queue.transactional.mmap.meta;

import java.io.IOException;
import java.util.UUID;

public interface QueueMessageMetaSegment {

    QueueMessageMeta lockAndPop() throws IOException;

    QueueMessageMeta lock(UUID uuid) throws IOException;

    QueueMessageMeta unlockAndDelete(UUID uuid) throws IOException;

    QueueMessageMeta unlockAndQueue(UUID uuid) throws IOException;

    boolean writeLock(UUID uuid) throws IOException;

    boolean writeMeta(QueueMessageMeta meta) throws IOException;

    QueueMessageMeta readMeta(UUID uuid) throws IOException;
}
