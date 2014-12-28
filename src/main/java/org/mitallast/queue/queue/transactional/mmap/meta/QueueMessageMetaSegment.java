package org.mitallast.queue.queue.transactional.mmap.meta;

import java.io.IOException;
import java.util.UUID;

public interface QueueMessageMetaSegment {

    boolean writeLock(UUID uuid) throws IOException;

    boolean writeMeta(QueueMessageMeta meta) throws IOException;

    QueueMessageMeta readMeta(UUID uuid) throws IOException;
}
