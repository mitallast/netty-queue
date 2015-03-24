package org.mitallast.queue.queue.transactional.mmap;

import com.google.common.collect.ImmutableList;
import org.mitallast.queue.QueueException;
import org.mitallast.queue.common.mmap.MemoryMappedFileFactory;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.AbstractQueueComponent;
import org.mitallast.queue.queue.Queue;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.QueueMessageUuidDuplicateException;
import org.mitallast.queue.queue.transactional.QueueTransaction;
import org.mitallast.queue.queue.transactional.TransactionalQueueService;
import org.mitallast.queue.queue.transactional.memory.MemoryQueueTransaction;
import org.mitallast.queue.queue.transactional.mmap.data.MMapQueueMessageAppendSegment;
import org.mitallast.queue.queue.transactional.mmap.data.QueueMessageAppendSegment;
import org.mitallast.queue.queue.transactional.mmap.meta.MMapQueueMessageMetaSegment;
import org.mitallast.queue.queue.transactional.mmap.meta.QueueMessageMetaSegment;
import org.mitallast.queue.queues.stats.QueueStats;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

public class MMapTransactionalQueueService extends AbstractQueueComponent implements TransactionalQueueService {

    private final String workDir;
    private final int segmentMaxSize;
    private final float segmentLoadFactor;
    private final ReentrantLock segmentsLock = new ReentrantLock();
    private MemoryMappedFileFactory mmapFileFactory;
    private volatile ImmutableList<QueueMessageSegment> segments = ImmutableList.of();

    public MMapTransactionalQueueService(Settings settings, Settings queueSettings, Queue queue) {
        super(settings, queueSettings, queue);
        workDir = this.settings.get("work_dir", "data");
        segmentMaxSize = this.settings.getAsInt("segment.max_size", 655360);
        segmentLoadFactor = this.settings.getAsFloat("segment.load_factor", 0.75f);
    }

    @Override
    protected void doStart() throws QueueException {
        segments = ImmutableList.of();
        try {
            File file = new File(workDir, queue.getName());
            if (!file.exists() && !file.mkdirs()) {
                throw new IOException("Error create dir " + file);
            }
            mmapFileFactory = new MemoryMappedFileFactory(settings, file);
        } catch (IOException e) {
            throw new QueueException(e);
        }
    }

    @Override
    protected void doStop() throws QueueException {
        for (QueueMessageSegment segment : segments) {
            try {
                segment.close();
            } catch (IOException e) {
                throw new QueueException(e);
            }
        }
        segments = null;
        mmapFileFactory = null;
    }

    @Override
    protected void doClose() throws QueueException {
    }

    @Override
    public QueueTransaction transaction(String id) throws IOException {
        return new MemoryQueueTransaction(id, this);
    }

    @Override
    public QueueMessage get(UUID uuid) throws IOException {
        final ImmutableList<QueueMessageSegment> current = this.segments;
        final int size = current.size();
        for (int i = 0; i < size; i++) {
            final QueueMessageSegment segment = current.get(i);
            QueueMessage queueMessage = segment.get(uuid);
            if (queueMessage != null) {
                return queueMessage;
            }
        }
        return null;
    }

    @Override
    public QueueMessage lock(UUID uuid) throws IOException {
        final ImmutableList<QueueMessageSegment> current = this.segments;
        final int size = current.size();
        for (int i = 0; i < size; i++) {
            final QueueMessageSegment segment = current.get(i);
            QueueMessage queueMessage = segment.lock(uuid);
            if (queueMessage != null) {
                return queueMessage;
            }
        }
        return null;
    }

    @Override
    public QueueMessage lockAndPop() throws IOException {
        final ImmutableList<QueueMessageSegment> current = this.segments;
        final int size = current.size();
        for (int i = 0; i < size; i++) {
            final QueueMessageSegment segment = current.get(i);
            QueueMessage queueMessage = segment.lockAndPop();
            if (queueMessage != null) {
                return queueMessage;
            }
        }
        return null;
    }

    @Override
    public QueueMessage unlockAndDelete(UUID uuid) throws IOException {
        final ImmutableList<QueueMessageSegment> current = this.segments;
        final int size = current.size();
        for (int i = 0; i < size; i++) {
            final QueueMessageSegment segment = current.get(i);
            QueueMessage queueMessage = segment.unlockAndDelete(uuid);
            if (queueMessage != null) {
                return queueMessage;
            }
        }
        return null;
    }

    @Override
    public QueueMessage unlockAndRollback(UUID uuid) throws IOException {
        final ImmutableList<QueueMessageSegment> current = this.segments;
        final int size = current.size();
        for (int i = 0; i < size; i++) {
            final QueueMessageSegment segment = current.get(i);
            QueueMessage queueMessage = segment.unlockAndRollback(uuid);
            if (queueMessage != null) {
                return queueMessage;
            }
        }
        return null;
    }

    @Override
    public boolean push(QueueMessage queueMessage) throws IOException {
        final UUID uuid = queueMessage.getUuid();
        while (true) {
            final ImmutableList<QueueMessageSegment> current = this.segments;
            final int size = current.size();
            for (int i = 0; i < size; i++) {
                final QueueMessageSegment segment = current.get(i);
                if (segment.insert(uuid)) {
                    if (segment.writeLock(uuid)) {
                        segment.writeMessage(queueMessage);
                        return true;
                    } else {
                        throw new QueueMessageUuidDuplicateException(uuid);
                    }
                }
            }
            addSegment(current);
        }
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public QueueStats stats() throws IOException {
        return null;
    }

    private void addSegment(ImmutableList<QueueMessageSegment> current) throws IOException {
        // create new segment
        if (segmentsLock.tryLock()) {
            try {
                if (this.segments == current) {
                    this.segments = ImmutableList.<QueueMessageSegment>builder()
                        .addAll(current)
                        .add(createSegment())
                        .build();
                }
            } finally {
                segmentsLock.unlock();
            }
        }
    }

    private QueueMessageAppendSegment createAppendSegment() throws IOException {
        return new MMapQueueMessageAppendSegment(
            mmapFileFactory.createFile("data")
        );
    }

    private QueueMessageMetaSegment createMetaSegment() throws IOException {
        return new MMapQueueMessageMetaSegment(
            mmapFileFactory.createFile("meta"),
            segmentMaxSize,
            segmentLoadFactor
        );
    }

    private QueueMessageSegment createSegment() throws IOException {
        logger.info("create new segment");
        return new QueueMessageSegment(
            createAppendSegment(),
            createMetaSegment()
        );
    }
}
