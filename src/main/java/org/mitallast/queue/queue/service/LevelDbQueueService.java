package org.mitallast.queue.queue.service;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.*;
import org.mitallast.queue.QueueException;
import org.mitallast.queue.QueueRuntimeException;
import org.mitallast.queue.common.Files;
import org.mitallast.queue.common.UUIDs;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.Queue;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.QueueMessageUuidDuplicateException;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

public class LevelDbQueueService extends AbstractQueueService {

    private final static String LEVEL_DB_DIR = "level_db";
    private final Options options = new Options();
    private final WriteOptions writeOptions = new WriteOptions();
    private final ReadOptions readOptions = new ReadOptions();
    private final ReentrantLock lock = new ReentrantLock();
    private final String levelDbDir;
    private final ConcurrentLinkedQueue<UUID> keyQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentHashMap<UUID, UUID> keyMap = new ConcurrentHashMap<>();
    private String workDir;
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
            UUID uuid = UUIDs.generateRandom();
            message.setUuid(uuid);
            levelDb.put(toBytes(uuid), message.getSource(), writeOptions);
            assert keyMap.put(uuid, uuid) == null;
            keyQueue.add(uuid);
        } else {
            UUID uuid = message.getUuid();
            if (keyMap.putIfAbsent(uuid, uuid) == null) {
                levelDb.put(toBytes(uuid), message.getSource(), writeOptions);
                keyQueue.add(uuid);
            } else {
                throw new QueueMessageUuidDuplicateException(message.getUuid());
            }
        }
    }

    @Override
    public QueueMessage dequeue() {
        UUID uuid;
        while ((uuid = keyQueue.poll()) != null) {
            if (keyMap.remove(uuid) != null) {
                byte[] key = toBytes(uuid);
                byte[] msg = levelDb.get(key);
                if (msg != null) {
                    levelDb.delete(key);
                    return new QueueMessage(uuid, msg);
                }
            }
        }
        return null;
    }

    @Override
    public QueueMessage peek() {
        UUID uuid;
        while ((uuid = keyQueue.peek()) != null) {
            byte[] key = toBytes(uuid);
            byte[] msg = levelDb.get(key);
            if (msg != null) {
                return new QueueMessage(uuid, msg);
            }
        }
        return null;
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
        return keyQueue.size();
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
            File levelDbDirFile = new File(levelDbDir);
            if (!levelDbDirFile.exists()) {
                if (!levelDbDirFile.mkdirs()) {
                    throw new IOException("Error create " + levelDbDirFile);
                }
            }
            logger.info("start level db queue at [{}]", levelDbDirFile);
            levelDb = JniDBFactory.factory.open(levelDbDirFile, options);
            keyQueue.clear();
            keyMap.clear();
            try (DBIterator iterator = levelDb.iterator(readOptions)) {
                iterator.seekToFirst();
                while (iterator.hasNext()) {
                    Map.Entry<byte[], byte[]> entry = iterator.next();
                    UUID uuid = toUUID(entry.getKey());
                    keyQueue.add(uuid);
                    keyMap.put(uuid, uuid);
                }
            }
        } catch (IOException e) {
            throw new QueueRuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void doStop() throws QueueException {
        lock.lock();
        try {
            keyQueue.clear();
            keyMap.clear();
            levelDb.close();
            levelDb = null;
        } catch (IOException e) {
            throw new QueueRuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void doClose() throws QueueException {
    }

    public UUID toUUID(byte[] bytes) {
        return new UUID(toLong(bytes, 0), toLong(bytes, 8));
    }

    public byte[] toBytes(UUID uuid) {
        byte[] bytes = new byte[16];
        toBytes(bytes, 0, uuid.getMostSignificantBits());
        toBytes(bytes, 8, uuid.getLeastSignificantBits());
        return bytes;
    }

    public long toLong(byte[] bytes, int start) {
        return (bytes[start] & 0xFFL) << 56
                | (bytes[start + 1] & 0xFFL) << 48
                | (bytes[start + 2] & 0xFFL) << 40
                | (bytes[start + 3] & 0xFFL) << 32
                | (bytes[start + 4] & 0xFFL) << 24
                | (bytes[start + 5] & 0xFFL) << 16
                | (bytes[start + 6] & 0xFFL) << 8
                | (bytes[start + 7] & 0xFFL);
    }

    public void toBytes(byte[] bytes, int start, long value) {
        for (int i = 7 + start; i >= start; i--) {
            bytes[i] = (byte) (value & 0xffL);
            value >>= 8;
        }
    }
}
