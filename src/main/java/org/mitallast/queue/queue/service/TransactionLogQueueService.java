package org.mitallast.queue.queue.service;

import org.mitallast.queue.QueueException;
import org.mitallast.queue.QueueRuntimeException;
import org.mitallast.queue.common.Files;
import org.mitallast.queue.common.UUIDs;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.Queue;
import org.mitallast.queue.queue.QueueMessage;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionLogQueueService extends AbstractQueueService {

    private final static String LOG_FILE = "transaction_log.bin";
    private final static String LOG_META_FILE = "transaction_log_meta.bin";

    private final ReentrantLock lock = new ReentrantLock();
    private String workDir;
    private String logFile;
    private String logMetaFile;
    private FileQueueMessageAppendTransactionLog transactionLog;

    public TransactionLogQueueService(Settings settings, Settings queueSettings, Queue queue) {
        super(settings, queueSettings, queue);
        workDir = this.settings.get("work_dir");
        if (!workDir.endsWith(File.separator)) {
            workDir += File.separator;
        }
        workDir += queue.getName() + File.separator;
        logFile = workDir + LOG_FILE;
        logMetaFile = workDir + LOG_META_FILE;
    }

    @Override
    protected void doStart() throws QueueException {
        lock.lock();
        try {
            File dataFile = new File(logFile);
            File metaFile = new File(logMetaFile);
            if (!dataFile.exists()) {
                if (!dataFile.getParentFile().exists()) {
                    if (!dataFile.getParentFile().mkdirs()) {
                        throw new IOException("Error create " + dataFile.getParent());
                    }
                }
                if (!dataFile.createNewFile()) {
                    throw new IOException("Error create " + dataFile);
                }
                if (!metaFile.createNewFile()) {
                    throw new IOException("Error create " + metaFile);
                }
                transactionLog = new FileQueueMessageAppendTransactionLog(metaFile, dataFile);
                transactionLog.initializeNew();
            } else {
                transactionLog = new FileQueueMessageAppendTransactionLog(metaFile, dataFile);
                transactionLog.initializeExists();
            }
        } catch (IOException e) {
            throw new QueueException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void doStop() throws QueueException {
        lock.lock();
        try {
            transactionLog.close();
            transactionLog = null;
        } catch (IOException e) {
            throw new QueueException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void doClose() throws QueueException {
    }

    @Override
    public void enqueue(QueueMessage message) {
        try {
            if (message.getUuid() == null) {
                message.setUuid(UUIDs.generateRandom());
            }
            transactionLog.putMessage(message);
        } catch (IOException e) {
            throw new QueueException(e);
        }
    }

    @Override
    public QueueMessage dequeue() {
        try {
            return transactionLog.dequeueMessage();
        } catch (IOException e) {
            throw new QueueException(e);
        }
    }

    @Override
    public QueueMessage peek() {
        try {
            return transactionLog.peekMessage();
        } catch (IOException e) {
            throw new QueueException(e);
        }
    }

    @Override
    public QueueMessage get(UUID uuid) {
        try {
            return transactionLog.readMessage(uuid);
        } catch (IOException e) {
            throw new QueueException(e);
        }
    }

    @Override
    public void delete(UUID uuid) {
        try {
            transactionLog.markMessageDeleted(uuid);
        } catch (IOException e) {
            throw new QueueException(e);
        }
    }

    @Override
    public long size() {
        return transactionLog.getMessageCount();
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
}
