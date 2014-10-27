package org.mitallast.queue.queue.service;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.*;
import org.mitallast.queue.QueueException;
import org.mitallast.queue.QueueRuntimeException;
import org.mitallast.queue.common.Files;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.AbstractQueueComponent;
import org.mitallast.queue.queue.Queue;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.QueueMessageUuidDuplicateException;
import org.mitallast.queue.queues.stats.QueueStats;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

public class LevelDbQueueService extends AbstractQueueComponent implements QueueService {

    private final static String LEVEL_DB_DIR = "level_db";
    private final Options options = new Options();
    private final WriteOptions writeOptions = new WriteOptions();
    private final ReadOptions readOptions = new ReadOptions();
    private final ReentrantLock lock = new ReentrantLock();
    private String workDir;
    private final String levelDbDir;
    private DB levelDb;

    public LevelDbQueueService(Settings settings, Settings queueSettings, Queue queue) {
        super(settings, queueSettings, queue);
        workDir = this.settings.get("work_dir");
        if (!workDir.endsWith(File.separator)) {
            workDir += File.separator;
        }
        workDir += queue.getName() + File.separator;
        levelDbDir = workDir + LEVEL_DB_DIR;

        options.createIfMissing(true);
        options.maxOpenFiles(4096);
        options.blockSize(65536);
        options.verifyChecksums(false);
        options.paranoidChecks(false);

        writeOptions.snapshot(false);
        writeOptions.sync(false);

        readOptions.fillCache(false);
        readOptions.verifyChecksums(false);
    }

    @Override
    public void enqueue(QueueMessage message) {
        if (message.getUuid() == null) {
            // write without lock
            message.setUuid(UUID.randomUUID());
            levelDb.put(toBytes(message.getUuid()), message.getSource(), writeOptions);
        } else {
            // write with lock
            byte[] uuid = toBytes(message.getUuid());
            lock.lock();
            try {
                if (levelDb.get(uuid, readOptions) != null) {
                    throw new QueueMessageUuidDuplicateException(message.getUuid());
                }
                levelDb.put(uuid, message.getSource(), writeOptions);
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public QueueMessage dequeue() {
        lock.lock();
        try (DBIterator iterator = levelDb.iterator(readOptions)) {
            iterator.seekToFirst();
            if (!iterator.hasNext()) {
                return null;
            }
            Map.Entry<byte[], byte[]> entry = iterator.next();
            levelDb.delete(entry.getKey(), writeOptions);
            return new QueueMessage(toUUID(entry.getKey()), entry.getValue());
        } catch (IOException e) {
            throw new QueueRuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public QueueMessage peek() {
        try (DBIterator iterator = levelDb.iterator(readOptions)) {
            iterator.seekToFirst();
            if (!iterator.hasNext()) {
                return null;
            }
            Map.Entry<byte[], byte[]> entry = iterator.next();
            return new QueueMessage(toUUID(entry.getKey()), entry.getValue());
        } catch (IOException e) {
            throw new QueueRuntimeException(e);
        }
    }

    @Override
    public QueueMessage get(UUID uuid) {
        byte[] msg = levelDb.get(toBytes(uuid));
        if (msg != null) {
            return new QueueMessage(uuid, msg);
        } else {
            return null;
        }
    }

    @Override
    public void delete(UUID uuid) {
        levelDb.delete(toBytes(uuid), writeOptions);
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public void removeQueue() {
        lock.lock();
        try {
            logger.info("close queue");
            close();
            logger.info("delete directory");
            Files.deleteDirectory(new File(workDir));
            logger.info("directory deleted");
        } catch (Throwable e) {
            throw new QueueRuntimeException(e);
        } finally {
            lock.unlock();
        }
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
                logger.info("start level db queue at [{}]", levelDbDirFile);
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

    public static UUID toUUID(byte[] bytes) {
        return new UUID(toLong(bytes, 0), toLong(bytes, 8));
    }

    public static byte[] toBytes(UUID uuid) {
        byte[] bytes = new byte[16];
        toBytes(bytes, 0, uuid.getMostSignificantBits());
        toBytes(bytes, 8, uuid.getLeastSignificantBits());
        return bytes;
    }

    public static long toLong(byte[] bytes, int start) {
        return (bytes[start] & 0xFFL) << 56
                | (bytes[start + 1] & 0xFFL) << 48
                | (bytes[start + 2] & 0xFFL) << 40
                | (bytes[start + 3] & 0xFFL) << 32
                | (bytes[start + 4] & 0xFFL) << 24
                | (bytes[start + 5] & 0xFFL) << 16
                | (bytes[start + 6] & 0xFFL) << 8
                | (bytes[start + 7] & 0xFFL);
    }

    public static void toBytes(byte[] bytes, int start, long value) {
        for (int i = 7 + start; i >= start; i--) {
            bytes[i] = (byte) (value & 0xffL);
            value >>= 8;
        }
    }
}
