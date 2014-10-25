package org.mitallast.queue.queue.service;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.*;
import org.mitallast.queue.QueueException;
import org.mitallast.queue.QueueRuntimeException;
import org.mitallast.queue.common.bigqueue.Files;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.*;
import org.mitallast.queue.queues.stats.QueueStats;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

public class LevelDbQueueService extends AbstractQueueComponent implements QueueService<String> {

    private final static String LEVEL_DB_DIR = "level_db";
    private final Options options = new Options();
    private final WriteOptions writeOptions = new WriteOptions();
    private final ReadOptions readOptions = new ReadOptions();

    {
        options.createIfMissing(true);
        options.maxOpenFiles(4096);
        options.blockSize(65536);
        options.verifyChecksums(false);
        options.paranoidChecks(false);

        writeOptions.snapshot(false);
        writeOptions.sync(false);

        readOptions.fillCache(true);
        readOptions.verifyChecksums(false);
    }

    private final ReentrantLock lock = new ReentrantLock();
    private String workDir;
    private String levelDbDir;
    private DB levelDb;

    public LevelDbQueueService(Settings settings, Settings queueSettings, Queue queue) {
        super(settings, queueSettings, queue);
        workDir = this.settings.get("work_dir");
        if (!workDir.endsWith(File.separator)) {
            workDir += File.separator;
        }
        workDir += queue.getName() + File.separator;
        levelDbDir = workDir + LEVEL_DB_DIR;
    }

    @Override
    public long enqueue(QueueMessage<String> message) {
        if (message.getUid() == null) {
            // write without lock
            message.setUid(UUID.randomUUID().toString());
            byte[] uid = message.getUid().getBytes();
            byte[] msg = message.getMessage().getBytes();
            levelDb.put(uid, msg, writeOptions);
            return 0;
        } else {
            // write with lock
            byte[] uid = message.getUid().getBytes();
            byte[] msg = message.getMessage().getBytes();
            lock.lock();
            try {
                if (levelDb.get(uid, readOptions) != null) {
                    throw new QueueMessageUidDuplicateException(message.getUid());
                }
                levelDb.put(uid, msg, writeOptions);
                return 0;
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public QueueMessage<String> dequeue() {
        lock.lock();
        try {
            try (DBIterator iterator = levelDb.iterator(readOptions)) {
                if (!iterator.hasNext()) {
                    return null;
                }
                Map.Entry<byte[], byte[]> entry = iterator.next();
                levelDb.delete(entry.getKey(), writeOptions);
                return new QueueMessage<>(new String(entry.getValue()), new String(entry.getKey()));
            }
        } catch (IOException e) {
            throw new QueueRuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public QueueMessage<String> peek() {
        lock.lock();
        try {
            try (DBIterator iterator = levelDb.iterator(readOptions)) {
                if (!iterator.hasNext()) {
                    return null;
                }
                Map.Entry<byte[], byte[]> entry = iterator.next();
                return new QueueMessage<>(new String(entry.getValue()), new String(entry.getKey()));
            }
        } catch (IOException e) {
            throw new QueueRuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public QueueType type() {
        return QueueType.LEVEL_DB;
    }

    @Override
    public void removeQueue() {
        lock.lock();
        try {
            logger.info("close queue");
            close();
            logger.info("delete directory");
            Files.deleteDirectory(new File(levelDbDir));
            logger.info("directory deleted");
        } catch (Throwable e) {
            throw new QueueRuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isSupported(QueueMessage message) {
        return message.getMessage() instanceof String;
    }

    @Override
    protected void doStart() throws QueueException {
        lock.lock();
        try {
            try {
                File levelDbDirFile = new File(levelDbDir);
                if (!levelDbDirFile.exists()) {
                    if (!levelDbDirFile.mkdirs()) {
                        throw new IOException("Error create " + levelDbDirFile);
                    }
                }
                levelDb = JniDBFactory.factory.open(levelDbDirFile, options);
            } catch (IOException e) {
                throw new QueueRuntimeException(e);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void doStop() throws QueueException {
        lock.lock();
        try {
            this.levelDb.close();
            this.levelDb = null;
        } catch (IOException e) {
            throw new QueueRuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void doClose() throws QueueException {
    }

    @Override
    public QueueStats stats() {
        QueueStats stats = new QueueStats();
        stats.setQueue(queue);
        stats.setSize(size());
        return stats;
    }
}
